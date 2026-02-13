"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { API_URL, fetchWithRetry } from "@/lib/api";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  citations?: string[];
}

// ---------------------------------------------------------------------------
// Conversation context helpers
// ---------------------------------------------------------------------------

/** Truncate assistant messages so the payload stays small for Perplexity. */
function truncateAssistant(content: string, maxLen = 300): string {
  if (content.length <= maxLen) return content;
  return content.slice(0, maxLen) + "…";
}

/** Build the messages payload: always include conversation history so pronouns
 *  like "it" resolve correctly, but keep assistant text short to avoid 400s. */
function buildPayload(allMessages: ChatMessage[]): ChatMessage[] {
  // Take last 6 messages for context window
  const recent = allMessages.slice(-6);
  return recent.map((m) =>
    m.role === "assistant"
      ? { ...m, content: truncateAssistant(m.content), citations: undefined }
      : m
  );
}

// ---------------------------------------------------------------------------
// Markdown renderer
// ---------------------------------------------------------------------------

function renderMarkdown(text: string): string {
  // Escape HTML entities
  let html = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  // Headings: ## Heading → <h3>
  html = html.replace(/^## (.+)$/gm, "<h3>$1</h3>");

  // Bold: **text** → <strong>
  html = html.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");

  // Italic: *text* → <em>
  html = html.replace(/\*(.+?)\*/g, "<em>$1</em>");

  // Links: [text](url) → <a>
  html = html.replace(
    /\[(.+?)\]\((.+?)\)/g,
    '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>'
  );

  // List items: collect consecutive lines starting with "- "
  html = html.replace(
    /((?:^- .+$\n?)+)/gm,
    (block) => {
      const items = block
        .trim()
        .split("\n")
        .map((line) => `<li>${line.replace(/^- /, "")}</li>`)
        .join("");
      return `<ul>${items}</ul>`;
    }
  );

  // Double newlines → paragraph breaks
  html = html.replace(/\n\n/g, "</p><p>");

  // Single newlines → <br/>
  html = html.replace(/\n/g, "<br/>");

  // Wrap in paragraph
  html = `<p>${html}</p>`;

  // Clean up empty paragraphs
  html = html.replace(/<p><\/p>/g, "");

  return html;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function AISearch() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingText, setStreamingText] = useState("");
  const [streamingCitations, setStreamingCitations] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll to bottom when messages or streaming text change
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, streamingText]);

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // -- Send message ---------------------------------------------------------
  const sendMessage = useCallback(async () => {
    const trimmed = input.trim();
    if (!trimmed || isStreaming) return;

    setError(null);
    const userMessage: ChatMessage = { role: "user", content: trimmed };
    const updatedMessages = [...messages, userMessage];
    setMessages(updatedMessages);
    setInput("");
    setIsStreaming(true);
    setStreamingText("");
    setStreamingCitations([]);

    // Always send conversation history so pronouns ("it", "that") resolve
    const payload = { messages: buildPayload(updatedMessages) };

    try {
      const response = await fetchWithRetry(`${API_URL}/ai/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errText = await response.text().catch(() => response.statusText);
        throw new Error(`API error ${response.status}: ${errText}`);
      }

      if (!response.body) {
        throw new Error("No response body");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let accumulated = "";
      let citations: string[] = [];
      let currentEvent = "";
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Process complete lines
        const lines = buffer.split("\n");
        // Keep the last potentially incomplete line in the buffer
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (line.startsWith("event: ")) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith("data: ")) {
            const dataStr = line.slice(6);
            try {
              const data = JSON.parse(dataStr);

              if (currentEvent === "delta") {
                const text = data.text ?? data.content ?? "";
                accumulated += text;
                setStreamingText(accumulated);
              } else if (currentEvent === "done") {
                citations = data.citations ?? [];
                setStreamingCitations(citations);
              } else if (currentEvent === "error") {
                setError(data.message ?? data.error ?? "Stream error");
              }
            } catch {
              // Non-JSON data line, skip
            }
          }
        }
      }

      // Finalize: add assistant message
      if (accumulated) {
        const assistantMessage: ChatMessage = {
          role: "assistant",
          content: accumulated,
          citations: citations.length > 0 ? citations : undefined,
        };
        setMessages((prev) => [...prev, assistantMessage]);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      setError(msg);
    } finally {
      setIsStreaming(false);
      setStreamingText("");
      setStreamingCitations([]);
    }
  }, [input, isStreaming, messages]);

  // -- Key handler ----------------------------------------------------------
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    },
    [sendMessage]
  );

  // -- Render ---------------------------------------------------------------
  return (
    <div className="ai-chat-container">
      {/* Messages area */}
      <div className="ai-chat-messages">
        {messages.length === 0 && !isStreaming && (
          <div className="ai-chat-empty">
            Ask about any ticker, market, or macro event
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`ai-chat-message ${msg.role}`}>
            <div
              className="ai-chat-message-content"
              dangerouslySetInnerHTML={{
                __html: renderMarkdown(msg.content),
              }}
            />
            {msg.citations && msg.citations.length > 0 && (
              <div className="ai-chat-meta">
                {msg.citations.map((cite, j) => (
                  <a
                    key={j}
                    href={cite}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    [{j + 1}]
                  </a>
                ))}
              </div>
            )}
          </div>
        ))}

        {/* Streaming message */}
        {isStreaming && streamingText && (
          <div className="ai-chat-message assistant">
            <div
              className="ai-chat-message-content"
              dangerouslySetInnerHTML={{
                __html: renderMarkdown(streamingText),
              }}
            />
            <span className="ai-chat-streaming-cursor" />
          </div>
        )}

        {/* Streaming with no text yet */}
        {isStreaming && !streamingText && (
          <div className="ai-chat-message assistant">
            <div className="ai-chat-message-content">
              <span className="ai-chat-streaming-cursor" />
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Error banner */}
      {error && (
        <div className="ai-chat-error">
          {error}
        </div>
      )}

      {/* Input bar */}
      <div className="ai-chat-input-bar">
        <input
          ref={inputRef}
          className="ai-chat-input"
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask anything about markets..."
          disabled={isStreaming}
        />
        <button
          className="ai-chat-send-btn"
          onClick={sendMessage}
          disabled={isStreaming || !input.trim()}
        >
          Send
        </button>
      </div>
    </div>
  );
}
