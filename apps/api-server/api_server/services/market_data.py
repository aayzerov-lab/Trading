"""Fetch exact market price data from Yahoo Finance via yfinance.

Used to enrich AI Search queries with authoritative price data before
sending to Perplexity for news/analysis.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import structlog
import yfinance as yf

logger = structlog.get_logger()

_ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Ticker extraction
# ---------------------------------------------------------------------------

_AMBIGUOUS = {"A", "ALL", "ARE", "AT", "BE", "BIG", "CAN", "CAR", "CEO", "DD",
              "DO", "IT", "MAN", "NOW", "ON", "ONE", "OR", "OUT", "RUN", "SEE",
              "SO", "TWO", "UK", "UP", "US", "WAS", "HAS", "FOR", "LOW", "NEW",
              "NEXT", "OPEN", "PLAY", "POST", "REAL", "SUN", "TRUE", "WELL"}

# Company name → ticker(s) mapping for natural language queries
# Covers all S&P 500 constituents, major ETFs/indices, and popular non-S&P stocks
_COMPANY_NAMES: dict[str, str] = {
    # --- S&P 500 companies (with aliases) ---
    "3m": "MMM",
    "a. o. smith": "AOS", "a.o. smith": "AOS", "ao smith": "AOS",
    "abbott": "ABT", "abbott laboratories": "ABT",
    "abbvie": "ABBV",
    "accenture": "ACN",
    "adobe": "ADBE",
    "adm": "ADM", "archer daniels": "ADM", "archer daniels midland": "ADM",
    "adp": "ADP", "automatic data processing": "ADP",
    "advanced micro devices": "AMD", "amd": "AMD",
    "aes": "AES", "aes corporation": "AES",
    "aflac": "AFL",
    "agilent": "A", "agilent technologies": "A",
    "aig": "AIG", "american international group": "AIG",
    "air products": "APD", "air products and chemicals": "APD",
    "airbnb": "ABNB",
    "akamai": "AKAM", "akamai technologies": "AKAM",
    "albemarle": "ALB",
    "alexandria real estate": "ARE", "alexandria real estate equities": "ARE",
    "align technology": "ALGN", "invisalign": "ALGN",
    "allegion": "ALLE",
    "alliant energy": "LNT",
    "allstate": "ALL",
    "alphabet": "GOOG", "google": "GOOG", "googl": "GOOGL",
    "altria": "MO", "altria group": "MO",
    "amazon": "AMZN", "aws": "AMZN",
    "amcor": "AMCR",
    "ameren": "AEE",
    "american electric power": "AEP",
    "american express": "AXP", "amex": "AXP",
    "american tower": "AMT",
    "american water": "AWK", "american water works": "AWK",
    "ameriprise": "AMP", "ameriprise financial": "AMP",
    "amerisourcebergen": "COR",
    "ametek": "AME",
    "amgen": "AMGN",
    "amphenol": "APH",
    "analog devices": "ADI",
    "anthem": "ELV",
    "aon": "AON",
    "apa": "APA", "apa corporation": "APA", "apache": "APA",
    "apollo": "APO", "apollo global": "APO", "apollo global management": "APO",
    "apple": "AAPL",
    "applied materials": "AMAT",
    "applovin": "APP",
    "aptiv": "APTV",
    "arch capital": "ACGL", "arch capital group": "ACGL",
    "ares": "ARES", "ares management": "ARES",
    "arista": "ANET", "arista networks": "ANET",
    "arthur j. gallagher": "AJG", "gallagher": "AJG",
    "assurant": "AIZ",
    "at&t": "T", "att": "T",
    "atmos": "ATO", "atmos energy": "ATO",
    "autodesk": "ADSK",
    "autozone": "AZO",
    "avalonbay": "AVB", "avalonbay communities": "AVB",
    "avery dennison": "AVY",
    "axon": "AXON", "axon enterprise": "AXON",
    "bac": "BAC", "bank of america": "BAC", "bofa": "BAC",
    "baker hughes": "BKR",
    "ball corporation": "BALL",
    "bank of new york": "BK", "bank of new york mellon": "BK", "bny mellon": "BK",
    "baxter": "BAX", "baxter international": "BAX",
    "bd": "BDX", "becton dickinson": "BDX",
    "berkshire": "BRK-B", "berkshire hathaway": "BRK-B", "warren buffett": "BRK-B",
    "best buy": "BBY", "bestbuy": "BBY",
    "bio-techne": "TECH", "bio techne": "TECH",
    "biogen": "BIIB",
    "blackrock": "BLK",
    "blackstone": "BX",
    "block inc": "XYZ", "square": "XYZ",
    "bms": "BMY", "bristol myers": "BMY", "bristol myers squibb": "BMY", "bristol-myers squibb": "BMY",
    "boeing": "BA",
    "booking": "BKNG", "booking holdings": "BKNG", "priceline": "BKNG",
    "boston properties": "BXP", "bxp": "BXP",
    "boston scientific": "BSX",
    "broadcom": "AVGO",
    "broadridge": "BR", "broadridge financial": "BR",
    "brown & brown": "BRO", "brown and brown": "BRO",
    "brown-forman": "BF-B", "brown forman": "BF-B", "jack daniels": "BF-B",
    "builders firstsource": "BLDR",
    "bunge": "BG", "bunge global": "BG",
    "c.h. robinson": "CHRW", "ch robinson": "CHRW",
    "cadence": "CDNS", "cadence design": "CDNS", "cadence design systems": "CDNS",
    "camden property": "CPT", "camden property trust": "CPT",
    "campbells": "CPB", "campbell soup": "CPB", "campbell's": "CPB",
    "capital one": "COF",
    "cardinal health": "CAH",
    "carnival": "CCL", "carnival cruise": "CCL",
    "carrier": "CARR", "carrier global": "CARR",
    "carvana": "CVNA",
    "caterpillar": "CAT",
    "cboe": "CBOE", "cboe global markets": "CBOE",
    "cbre": "CBRE", "cbre group": "CBRE",
    "cdw": "CDW",
    "cencora": "COR",
    "centene": "CNC",
    "centerpoint": "CNP", "centerpoint energy": "CNP",
    "cf industries": "CF",
    "charles river": "CRL", "charles river laboratories": "CRL",
    "charles schwab": "SCHW", "schwab": "SCHW",
    "charter": "CHTR", "charter communications": "CHTR", "spectrum": "CHTR",
    "chase": "JPM",
    "chesapeake energy": "EXE",
    "chevron": "CVX",
    "chicago mercantile exchange": "CME",
    "chipotle": "CMG", "chipotle mexican grill": "CMG",
    "chubb": "CB",
    "church & dwight": "CHD", "church and dwight": "CHD",
    "ciena": "CIEN",
    "cigna": "CI",
    "cincinnati financial": "CINF",
    "cintas": "CTAS",
    "cisco": "CSCO",
    "citi": "C", "citibank": "C", "citigroup": "C",
    "citizens bank": "CFG", "citizens financial": "CFG",
    "clorox": "CLX",
    "cme": "CME", "cme group": "CME",
    "cms energy": "CMS",
    "coach": "TPR",
    "coca-cola": "KO", "coca cola": "KO", "coke": "KO",
    "cognizant": "CTSH",
    "coinbase": "COIN",
    "colgate": "CL", "colgate-palmolive": "CL", "colgate palmolive": "CL",
    "comcast": "CMCSA", "xfinity": "CMCSA",
    "comfort systems": "FIX",
    "con ed": "ED", "con edison": "ED", "consolidated edison": "ED",
    "conagra": "CAG", "conagra brands": "CAG",
    "conoco": "COP", "conocophillips": "COP",
    "constellation brands": "STZ",
    "constellation energy": "CEG",
    "cooper companies": "COO",
    "copart": "CPRT",
    "corning": "GLW",
    "corpay": "CPAY", "fleetcor": "CPAY",
    "corteva": "CTVA",
    "costar": "CSGP", "costar group": "CSGP",
    "costco": "COST",
    "coterra": "CTRA", "coterra energy": "CTRA",
    "crh": "CRH",
    "crowdstrike": "CRWD",
    "crown castle": "CCI",
    "csx": "CSX",
    "cummins": "CMI",
    "cvs": "CVS", "cvs health": "CVS",
    "d.r. horton": "DHI", "dr horton": "DHI",
    "danaher": "DHR",
    "darden": "DRI", "darden restaurants": "DRI", "olive garden": "DRI",
    "datadog": "DDOG",
    "davita": "DVA",
    "deckers": "DECK", "deckers brands": "DECK", "ugg": "DECK",
    "deere": "DE", "john deere": "DE",
    "dell": "DELL", "dell technologies": "DELL",
    "delta": "DAL", "delta air lines": "DAL", "delta airlines": "DAL",
    "devon": "DVN", "devon energy": "DVN",
    "dexcom": "DXCM",
    "diamondback": "FANG", "diamondback energy": "FANG",
    "digital realty": "DLR",
    "disney": "DIS", "walt disney": "DIS",
    "dollar general": "DG",
    "dollar tree": "DLTR",
    "dominion": "D", "dominion energy": "D",
    "dominos": "DPZ", "domino's": "DPZ", "domino's pizza": "DPZ",
    "doordash": "DASH",
    "dover": "DOV", "dover corporation": "DOV",
    "dow": "DOW", "dow inc": "DOW", "dow chemical": "DOW",
    "dte": "DTE", "dte energy": "DTE",
    "duke": "DUK", "duke energy": "DUK",
    "dupont": "DD",
    "ea": "EA", "ea sports": "EA", "electronic arts": "EA",
    "eaton": "ETN", "eaton corporation": "ETN",
    "ebay": "EBAY",
    "ecolab": "ECL",
    "edison international": "EIX",
    "edwards": "EW", "edwards lifesciences": "EW",
    "elevance": "ELV", "elevance health": "ELV",
    "eli lilly": "LLY", "lilly": "LLY",
    "emcor": "EME",
    "emerson": "EMR", "emerson electric": "EMR",
    "entergy": "ETR",
    "eog": "EOG", "eog resources": "EOG",
    "epam": "EPAM", "epam systems": "EPAM",
    "eqt": "EQT",
    "equifax": "EFX",
    "equinix": "EQIX",
    "equity residential": "EQR",
    "erie indemnity": "ERIE",
    "essex property": "ESS",
    "estee lauder": "EL",
    "everest group": "EG", "everest re": "EG",
    "evergy": "EVRG",
    "eversource": "ES", "eversource energy": "ES",
    "exelon": "EXC",
    "expand energy": "EXE",
    "expedia": "EXPE", "expedia group": "EXPE",
    "expeditors": "EXPD", "expeditors international": "EXPD",
    "extra space": "EXR", "extra space storage": "EXR",
    "exxon": "XOM", "exxon mobil": "XOM", "exxonmobil": "XOM",
    "f5": "FFIV", "f5 networks": "FFIV",
    "factset": "FDS",
    "fair isaac": "FICO", "fico": "FICO",
    "fastenal": "FAST",
    "federal realty": "FRT",
    "fedex": "FDX",
    "fidelity national": "FIS", "fis": "FIS",
    "fifth third": "FITB", "fifth third bancorp": "FITB",
    "first solar": "FSLR",
    "firstenergy": "FE", "first energy": "FE",
    "fiserv": "FISV",
    "ford": "F", "ford motor": "F",
    "fortinet": "FTNT",
    "fortive": "FTV",
    "fox corporation": "FOXA", "fox corp": "FOXA", "fox news": "FOXA",
    "franklin resources": "BEN", "franklin templeton": "BEN",
    "freeport": "FCX", "freeport-mcmoran": "FCX", "freeport mcmoran": "FCX",
    "garmin": "GRMN",
    "gartner": "IT",
    "ge aerospace": "GE", "general electric": "GE",
    "ge healthcare": "GEHC",
    "ge vernova": "GEV",
    "gen digital": "GEN", "norton lifelock": "GEN", "nortonlifelock": "GEN",
    "generac": "GNRC",
    "general dynamics": "GD",
    "general mills": "GIS",
    "general motors": "GM", "gm": "GM",
    "genuine parts": "GPC", "napa auto parts": "GPC",
    "gilead": "GILD", "gilead sciences": "GILD",
    "global payments": "GPN",
    "globe life": "GL",
    "godaddy": "GDDY",
    "goldman": "GS", "goldman sachs": "GS",
    "grainger": "GWW", "w.w. grainger": "GWW",
    "halliburton": "HAL",
    "hartford": "HIG", "the hartford": "HIG",
    "hasbro": "HAS",
    "hca": "HCA", "hca healthcare": "HCA",
    "healthpeak": "DOC", "healthpeak properties": "DOC",
    "heinz": "KHC", "kraft": "KHC", "kraft heinz": "KHC",
    "henry schein": "HSIC",
    "hershey": "HSY", "hersheys": "HSY",
    "hewlett packard": "HPQ", "hewlett-packard": "HPQ", "hp": "HPQ", "hp inc": "HPQ",
    "hewlett packard enterprise": "HPE", "hpe": "HPE",
    "hilton": "HLT", "hilton worldwide": "HLT",
    "hologic": "HOLX",
    "home depot": "HD", "homedepot": "HD",
    "honeywell": "HON",
    "hormel": "HRL", "hormel foods": "HRL",
    "host hotels": "HST", "host hotels & resorts": "HST",
    "howmet": "HWM", "howmet aerospace": "HWM",
    "hubbell": "HUBB",
    "humana": "HUM",
    "huntington bancshares": "HBAN", "huntington bank": "HBAN",
    "huntington ingalls": "HII",
    "ibkr": "IBKR", "interactive brokers": "IBKR",
    "ibm": "IBM",
    "ice": "ICE", "intercontinental exchange": "ICE",
    "idex": "IEX",
    "idexx": "IDXX", "idexx laboratories": "IDXX",
    "iff": "IFF", "international flavors": "IFF",
    "illinois tool works": "ITW", "itw": "ITW",
    "incyte": "INCY",
    "ingersoll rand": "IR", "ingersoll-rand": "IR",
    "instagram": "META",
    "insulet": "PODD", "omnipod": "PODD",
    "intel": "INTC",
    "international paper": "IP",
    "intuit": "INTU", "turbotax": "INTU", "quickbooks": "INTU",
    "intuitive": "ISRG", "intuitive surgical": "ISRG",
    "invesco": "IVZ",
    "invitation homes": "INVH",
    "iqvia": "IQV",
    "iron mountain": "IRM",
    "j&j": "JNJ", "jnj": "JNJ", "johnson & johnson": "JNJ", "johnson and johnson": "JNJ",
    "j.b. hunt": "JBHT", "jb hunt": "JBHT",
    "j.m. smucker": "SJM", "jm smucker": "SJM", "smucker": "SJM", "smuckers": "SJM",
    "jabil": "JBL",
    "jack henry": "JKHY",
    "jacobs": "J", "jacobs solutions": "J",
    "john deere": "DE",
    "johnson controls": "JCI",
    "jp morgan": "JPM", "jp morgan chase": "JPM", "jpmorgan": "JPM", "jpmorgan chase": "JPM",
    "kenvue": "KVUE",
    "keurig": "KDP", "keurig dr pepper": "KDP", "dr pepper": "KDP",
    "keybank": "KEY", "keycorp": "KEY",
    "keysight": "KEYS", "keysight technologies": "KEYS",
    "kfc": "YUM", "pizza hut": "YUM", "taco bell": "YUM", "yum": "YUM", "yum brands": "YUM",
    "kimberly-clark": "KMB", "kimberly clark": "KMB", "kleenex": "KMB",
    "kimco": "KIM", "kimco realty": "KIM",
    "kinder morgan": "KMI",
    "kkr": "KKR",
    "kla": "KLAC", "kla corporation": "KLAC",
    "kroger": "KR",
    "l3harris": "LHX", "l3 harris": "LHX", "harris corporation": "LHX",
    "labcorp": "LH", "laboratory corp": "LH",
    "lam research": "LRCX",
    "lamb weston": "LW",
    "las vegas sands": "LVS",
    "leidos": "LDOS",
    "lennar": "LEN",
    "lennox": "LII", "lennox international": "LII",
    "linde": "LIN",
    "live nation": "LYV", "ticketmaster": "LYV",
    "lockheed": "LMT", "lockheed martin": "LMT",
    "loews": "L",
    "lowes": "LOW", "lowe's": "LOW",
    "lululemon": "LULU",
    "lyondellbasell": "LYB", "lyondell basell": "LYB",
    "m&t bank": "MTB",
    "marathon": "MPC", "marathon petroleum": "MPC",
    "marriott": "MAR", "marriott international": "MAR",
    "marsh mclennan": "MRSH", "marsh & mclennan": "MRSH",
    "marshalls": "TJX",
    "martin marietta": "MLM",
    "masco": "MAS",
    "mastercard": "MA",
    "match group": "MTCH", "tinder": "MTCH",
    "mccormick": "MKC",
    "mcdonald": "MCD", "mcdonald's": "MCD", "mcdonalds": "MCD",
    "mckesson": "MCK",
    "medtronic": "MDT",
    "merck": "MRK",
    "meta": "META", "meta platforms": "META", "facebook": "META", "whatsapp": "META",
    "metlife": "MET",
    "mettler toledo": "MTD", "mettler-toledo": "MTD",
    "mgm": "MGM", "mgm resorts": "MGM",
    "microchip": "MCHP", "microchip technology": "MCHP",
    "micron": "MU", "micron technology": "MU",
    "microsoft": "MSFT",
    "mid-america apartment": "MAA",
    "moderna": "MRNA",
    "molina": "MOH", "molina healthcare": "MOH",
    "molson coors": "TAP",
    "mondelez": "MDLZ", "mondelez international": "MDLZ",
    "monolithic power": "MPWR", "monolithic power systems": "MPWR",
    "monster beverage": "MNST", "monster energy": "MNST",
    "moodys": "MCO", "moody's": "MCO",
    "morgan stanley": "MS",
    "mosaic": "MOS",
    "motorola": "MSI", "motorola solutions": "MSI",
    "msci": "MSCI",
    "mylan": "VTRS",
    "napa auto parts": "GPC",
    "nasdaq inc": "NDAQ",
    "netapp": "NTAP",
    "netflix": "NFLX",
    "newmont": "NEM",
    "news corp": "NWSA",
    "nextera": "NEE", "nextera energy": "NEE",
    "nike": "NKE",
    "nisource": "NI",
    "nordson": "NDSN",
    "norfolk southern": "NSC",
    "northern trust": "NTRS",
    "northrop": "NOC", "northrop grumman": "NOC",
    "norwegian cruise": "NCLH", "norwegian cruise line": "NCLH",
    "nrg": "NRG", "nrg energy": "NRG",
    "nucor": "NUE",
    "nvidia": "NVDA",
    "nvr": "NVR",
    "nxp": "NXPI", "nxp semiconductors": "NXPI",
    "o'reilly": "ORLY", "oreilly": "ORLY", "o'reilly automotive": "ORLY",
    "occidental": "OXY", "occidental petroleum": "OXY",
    "old dominion": "ODFL", "old dominion freight": "ODFL",
    "omnicom": "OMC",
    "on semiconductor": "ON", "onsemi": "ON",
    "oneok": "OKE",
    "oracle": "ORCL",
    "otis": "OTIS", "otis worldwide": "OTIS",
    "p&g": "PG", "procter & gamble": "PG", "procter and gamble": "PG",
    "paccar": "PCAR",
    "packaging corporation": "PKG", "packaging corp of america": "PKG",
    "palantir": "PLTR",
    "palo alto": "PANW", "palo alto networks": "PANW",
    "paramount": "PSKY", "paramount skydance": "PSKY", "skydance": "PSKY",
    "parker": "PH", "parker hannifin": "PH",
    "paychex": "PAYX",
    "paycom": "PAYC",
    "paypal": "PYPL",
    "pentair": "PNR",
    "pepsi": "PEP", "pepsico": "PEP",
    "perkinelmer": "RVTY",
    "pfizer": "PFE",
    "pg&e": "PCG", "pge": "PCG",
    "philip morris": "PM",
    "phillips 66": "PSX",
    "pinnacle west": "PNW",
    "pnc": "PNC", "pnc financial": "PNC",
    "pool corporation": "POOL",
    "pottery barn": "WSM",
    "ppg": "PPG", "ppg industries": "PPG",
    "ppl": "PPL", "ppl corporation": "PPL",
    "principal financial": "PFG",
    "progressive": "PGR",
    "prologis": "PLD",
    "prudential": "PRU", "prudential financial": "PRU",
    "pseg": "PEG", "public service enterprise": "PEG",
    "ptc": "PTC",
    "public storage": "PSA",
    "pulte": "PHM", "pultegroup": "PHM",
    "qualcomm": "QCOM",
    "quanta": "PWR", "quanta services": "PWR",
    "quest diagnostics": "DGX",
    "ralph lauren": "RL",
    "raymond james": "RJF",
    "raytheon": "RTX", "rtx": "RTX",
    "realty income": "O",
    "regency centers": "REG",
    "regeneron": "REGN",
    "regions bank": "RF", "regions financial": "RF",
    "republic services": "RSG",
    "resmed": "RMD",
    "revvity": "RVTY",
    "robinhood": "HOOD",
    "rockwell": "ROK", "rockwell automation": "ROK",
    "rollins": "ROL",
    "roper": "ROP", "roper technologies": "ROP",
    "ross": "ROST", "ross stores": "ROST",
    "royal caribbean": "RCL",
    "s&p global": "SPGI",
    "salesforce": "CRM",
    "sandisk": "SNDK",
    "sba communications": "SBAC",
    "schlumberger": "SLB", "slb": "SLB",
    "seagate": "STX", "seagate technology": "STX",
    "sempra": "SRE", "sempra energy": "SRE",
    "servicenow": "NOW",
    "sherwin-williams": "SHW", "sherwin williams": "SHW",
    "simon property": "SPG", "simon property group": "SPG",
    "skyworks": "SWKS", "skyworks solutions": "SWKS",
    "smurfit westrock": "SW", "westrock": "SW",
    "snap-on": "SNA",
    "solventum": "SOLV",
    "southern company": "SO",
    "southwest": "LUV", "southwest airlines": "LUV",
    "stanley": "SWK", "stanley black & decker": "SWK", "stanley black and decker": "SWK",
    "starbucks": "SBUX",
    "state street": "STT",
    "steel dynamics": "STLD",
    "steris": "STE",
    "stryker": "SYK",
    "super micro": "SMCI", "super micro computer": "SMCI", "supermicro": "SMCI",
    "synchrony": "SYF", "synchrony financial": "SYF",
    "synopsys": "SNPS",
    "sysco": "SYY",
    "t-mobile": "TMUS", "tmobile": "TMUS",
    "t. rowe price": "TROW", "t rowe price": "TROW",
    "take-two": "TTWO", "take two": "TTWO", "take-two interactive": "TTWO", "rockstar games": "TTWO",
    "tapestry": "TPR",
    "targa": "TRGP", "targa resources": "TRGP",
    "target": "TGT",
    "te connectivity": "TEL",
    "teledyne": "TDY", "teledyne technologies": "TDY",
    "teradyne": "TER",
    "tesla": "TSLA",
    "texas instruments": "TXN",
    "texas pacific land": "TPL",
    "textron": "TXT",
    "the trade desk": "TTD", "trade desk": "TTD",
    "thermo fisher": "TMO", "thermo fisher scientific": "TMO",
    "tj maxx": "TJX", "tjmaxx": "TJX", "tjx": "TJX",
    "tko": "TKO", "tko group": "TKO", "ufc": "TKO", "wwe": "TKO",
    "tractor supply": "TSCO",
    "trane": "TT", "trane technologies": "TT",
    "transdigm": "TDG",
    "travelers": "TRV",
    "trimble": "TRMB",
    "truist": "TFC", "truist financial": "TFC",
    "tyler technologies": "TYL",
    "tyson": "TSN", "tyson foods": "TSN",
    "u.s. bancorp": "USB", "us bancorp": "USB", "us bank": "USB",
    "uber": "UBER",
    "udr": "UDR",
    "ulta": "ULTA", "ulta beauty": "ULTA",
    "union pacific": "UNP",
    "united airlines": "UAL",
    "united health": "UNH", "united healthcare": "UNH",
    "unitedhealth": "UNH", "unitedhealth group": "UNH", "unitedhealthcare": "UNH",
    "united parcel service": "UPS", "ups": "UPS",
    "united rentals": "URI",
    "universal health services": "UHS",
    "valero": "VLO", "valero energy": "VLO",
    "ventas": "VTR",
    "veralto": "VLTO",
    "verisign": "VRSN",
    "verisk": "VRSK", "verisk analytics": "VRSK",
    "verizon": "VZ",
    "vertex": "VRTX", "vertex pharmaceuticals": "VRTX",
    "viatris": "VTRS",
    "vici": "VICI", "vici properties": "VICI",
    "visa": "V",
    "vistra": "VST",
    "vulcan": "VMC", "vulcan materials": "VMC",
    "w.r. berkley": "WRB", "wr berkley": "WRB",
    "wabtec": "WAB",
    "walmart": "WMT",
    "warner bros": "WBD", "warner brothers": "WBD", "warner bros discovery": "WBD", "discovery": "WBD",
    "waste management": "WM",
    "waters": "WAT", "waters corporation": "WAT",
    "wec energy": "WEC",
    "wells fargo": "WFC",
    "welltower": "WELL",
    "west pharma": "WST", "west pharmaceutical": "WST",
    "western digital": "WDC",
    "weyerhaeuser": "WY",
    "williams-sonoma": "WSM", "williams sonoma": "WSM",
    "williams companies": "WMB",
    "willis towers watson": "WTW", "wtw": "WTW",
    "workday": "WDAY",
    "wynn": "WYNN", "wynn resorts": "WYNN",
    "xcel": "XEL", "xcel energy": "XEL",
    "xylem": "XYL",
    "zebra": "ZBRA", "zebra technologies": "ZBRA",
    "zimmer": "ZBH", "zimmer biomet": "ZBH",
    "zoetis": "ZTS",
    # --- ETFs & Indices ---
    # Broad market
    "spy": "SPY", "s&p": "SPY", "s&p 500": "SPY", "s&p500": "SPY", "spdr": "SPY",
    "qqq": "QQQ", "nasdaq": "QQQ", "nasdaq 100": "QQQ", "invesco qqq": "QQQ",
    "dia": "DIA", "dow jones": "DIA", "diamonds": "DIA",
    "iwm": "IWM", "russell": "IWM", "russell 2000": "IWM",
    "voo": "VOO", "vanguard s&p 500": "VOO",
    "vti": "VTI", "total stock market": "VTI",
    # Volatility
    "vix": "^VIX", "cboe vix": "^VIX", "volatility index": "^VIX", "fear index": "^VIX",
    "uvxy": "UVXY",
    # Sector ETFs
    "xlf": "XLF", "financial select": "XLF", "financials etf": "XLF",
    "xle": "XLE", "energy select": "XLE", "energy etf": "XLE",
    "xlk": "XLK", "technology select": "XLK", "tech etf": "XLK",
    "xlv": "XLV", "health care select": "XLV", "healthcare etf": "XLV",
    "xli": "XLI", "industrial select": "XLI", "industrials etf": "XLI",
    "xlp": "XLP", "consumer staples select": "XLP", "staples etf": "XLP",
    "xly": "XLY", "consumer discretionary select": "XLY", "discretionary etf": "XLY",
    "xlc": "XLC", "communication services etf": "XLC",
    "xlu": "XLU", "utilities select": "XLU", "utilities etf": "XLU",
    "xlb": "XLB", "materials select": "XLB", "materials etf": "XLB",
    "xlre": "XLRE", "real estate select": "XLRE", "real estate etf": "XLRE",
    "xbi": "XBI", "biotech etf": "XBI",
    "soxx": "SOXX", "semiconductor etf": "SOXX",
    "smh": "SMH", "vaneck semiconductor": "SMH",
    # Commodity ETFs
    "gld": "GLD", "gold etf": "GLD", "spdr gold": "GLD",
    "slv": "SLV", "silver etf": "SLV",
    "uso": "USO", "oil etf": "USO", "united states oil": "USO",
    "gdx": "GDX", "gold miners etf": "GDX",
    # Bond ETFs
    "tlt": "TLT", "treasury bond etf": "TLT", "20 year treasury": "TLT",
    "ief": "IEF", "7-10 year treasury": "IEF",
    "shy": "SHY", "short term treasury": "SHY",
    "hyg": "HYG", "high yield bond": "HYG", "junk bond etf": "HYG",
    "lqd": "LQD", "investment grade bond": "LQD",
    "bnd": "BND", "total bond market": "BND",
    "agg": "AGG", "aggregate bond": "AGG",
    "tip": "TIP", "tips etf": "TIP",
    # International ETFs
    "eem": "EEM", "emerging markets etf": "EEM",
    "efa": "EFA", "developed markets etf": "EFA", "eafe": "EFA",
    "vwo": "VWO", "vanguard emerging markets": "VWO",
    "fxi": "FXI", "china etf": "FXI",
    "ewz": "EWZ", "brazil etf": "EWZ",
    "ewj": "EWJ", "japan etf": "EWJ",
    "kweb": "KWEB", "china internet etf": "KWEB",
    # REIT ETFs
    "iyr": "IYR", "reit etf": "IYR",
    "vnq": "VNQ", "vanguard real estate": "VNQ",
    # Leveraged/Inverse ETFs
    "tqqq": "TQQQ", "sqqq": "SQQQ",
    "spxl": "SPXL", "spxu": "SPXU",
    "soxl": "SOXL", "soxs": "SOXS",
    # Thematic ETFs
    "arkk": "ARKK", "ark innovation": "ARKK", "ark invest": "ARKK",
    # --- Popular non-S&P 500 stocks ---
    "alibaba": "BABA",
    "amc": "AMC", "amc entertainment": "AMC",
    "arm": "ARM", "arm holdings": "ARM",
    "asml": "ASML",
    "baidu": "BIDU",
    "berkshire hathaway a": "BRK-A",
    "byd": "BYDDY",
    "chewy": "CHWY",
    "docusign": "DOCU",
    "draftkings": "DKNG",
    "etsy": "ETSY",
    "gamestop": "GME",
    "jd": "JD", "jd.com": "JD",
    "lucid": "LCID", "lucid motors": "LCID",
    "nio": "NIO",
    "pdd": "PDD", "pinduoduo": "PDD",
    "pinterest": "PINS",
    "reddit": "RDDT",
    "rivian": "RIVN",
    "roblox": "RBLX",
    "shopify": "SHOP",
    "snap": "SNAP", "snapchat": "SNAP",
    "snowflake": "SNOW",
    "sofi": "SOFI",
    "spotify": "SPOT",
    "samsung": "SSNLF",
    "tencent": "TCEHY",
    "toyota": "TM",
    "tsmc": "TSM", "taiwan semiconductor": "TSM",
    "twilio": "TWLO",
    "unity": "U",
    "zillow": "Z",
    "zoom": "ZM", "zoom video": "ZM",
    # --- Crypto ---
    "bitcoin": "BTC-USD", "btc": "BTC-USD",
    "ethereum": "ETH-USD", "eth": "ETH-USD",
}

_TICKER_RE = re.compile(
    r'\$([A-Z]{1,5})'
    r'|(?<![a-zA-Z])'
    r'([A-Z]{2,5})'
    r'(?![a-zA-Z])'
)


def extract_tickers(text: str) -> list[str]:
    """Extract likely stock tickers from user message.

    Handles both uppercase ticker symbols (GOOG, $AAPL) and
    natural language company names (google, apple, tesla).
    """
    tickers: list[str] = []
    seen: set[str] = set()

    # 1. Check for company names in lowercase text
    lower = text.lower()
    # Sort by length descending so "goldman sachs" matches before "goldman"
    for name in sorted(_COMPANY_NAMES, key=len, reverse=True):
        if re.search(r'\b' + re.escape(name) + r'\b', lower):
            ticker = _COMPANY_NAMES[name]
            if ticker not in seen:
                seen.add(ticker)
                tickers.append(ticker)

    # 2. Check for uppercase ticker symbols
    for m in _TICKER_RE.finditer(text):
        ticker = m.group(1) or m.group(2)
        if not ticker or ticker in seen:
            continue
        if m.group(2) and ticker in _AMBIGUOUS:
            continue
        seen.add(ticker)
        tickers.append(ticker)

    return tickers


# ---------------------------------------------------------------------------
# Date extraction from natural language
# ---------------------------------------------------------------------------

_RELATIVE_PATTERNS: list[tuple[re.Pattern, object]] = [
    # "yesterday"
    (re.compile(r'\byesterday\b', re.I), lambda: timedelta(days=1)),
    # "X day(s) ago"
    (re.compile(r'\b(\d+)\s*days?\s*ago\b', re.I),
     lambda m: timedelta(days=int(m.group(1)))),
    # "X week(s) ago"
    (re.compile(r'\b(\d+)\s*weeks?\s*ago\b', re.I),
     lambda m: timedelta(weeks=int(m.group(1)))),
    # "X month(s) ago"
    (re.compile(r'\b(\d+)\s*months?\s*ago\b', re.I),
     lambda m: timedelta(days=int(m.group(1)) * 30)),
    # "X year(s) ago"
    (re.compile(r'\b(\d+)\s*years?\s*ago\b', re.I),
     lambda m: timedelta(days=int(m.group(1)) * 365)),
    # "one/a year ago"
    (re.compile(r'\b(?:one|a)\s+year\s*ago\b', re.I), lambda: timedelta(days=365)),
    # "one/a month ago"
    (re.compile(r'\b(?:one|a)\s+month\s*ago\b', re.I), lambda: timedelta(days=30)),
    # "one/a week ago"
    (re.compile(r'\b(?:one|a)\s+week\s*ago\b', re.I), lambda: timedelta(weeks=1)),
    # "last week"
    (re.compile(r'\blast\s+week\b', re.I), lambda: timedelta(weeks=1)),
    # "last month"
    (re.compile(r'\blast\s+month\b', re.I), lambda: timedelta(days=30)),
    # "last year"
    (re.compile(r'\blast\s+year\b', re.I), lambda: timedelta(days=365)),
]

# Explicit date patterns
_EXPLICIT_DATE_PATTERNS = [
    # "February 12, 2025" or "Feb 12, 2025"
    re.compile(
        r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?'
        r'|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?'
        r'|dec(?:ember)?)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s*(\d{4})\b', re.I
    ),
    # "12/31/2025" or "12-31-2025"
    re.compile(r'\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b'),
    # "2025-12-31"
    re.compile(r'\b(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})\b'),
]

_MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "september": 9, "oct": 10, "october": 10,
    "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def extract_dates(text: str) -> list[datetime]:
    """Extract date references from user text.

    Returns a list of datetime objects (in ET) that the user is asking about.
    """
    now = datetime.now(_ET)
    dates: list[datetime] = []

    # Check for "X ago from yesterday" pattern first
    from_yesterday = re.search(
        r'\b(\d+|one|a)\s*(year|month|week|day)s?\s*ago\s*from\s*yesterday\b',
        text, re.I
    )
    if from_yesterday:
        num_str = from_yesterday.group(1).lower()
        num = 1 if num_str in ("one", "a") else int(num_str)
        unit = from_yesterday.group(2).lower()
        base = now - timedelta(days=1)  # yesterday
        if unit == "year":
            delta = timedelta(days=num * 365)
        elif unit == "month":
            delta = timedelta(days=num * 30)
        elif unit == "week":
            delta = timedelta(weeks=num)
        else:
            delta = timedelta(days=num)
        dates.append(base - delta)
        return dates

    # Explicit dates
    for pat in _EXPLICIT_DATE_PATTERNS:
        for m in pat.finditer(text):
            try:
                groups = m.groups()
                if len(groups) == 3:
                    g0, g1, g2 = groups
                    # "Month DD, YYYY"
                    if g0.lower().rstrip(".") in _MONTH_MAP:
                        month = _MONTH_MAP[g0.lower().rstrip(".")]
                        dates.append(datetime(int(g2), month, int(g1), tzinfo=_ET))
                    # "MM/DD/YYYY"
                    elif len(g0) <= 2 and int(g2) > 31:
                        dates.append(datetime(int(g2), int(g0), int(g1), tzinfo=_ET))
                    # "YYYY-MM-DD"
                    elif int(g0) > 31:
                        dates.append(datetime(int(g0), int(g1), int(g2), tzinfo=_ET))
            except (ValueError, KeyError):
                continue

    # Relative dates
    for pat, delta_fn in _RELATIVE_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                delta = delta_fn(m) if m.groups() else delta_fn()
                dates.append(now - delta)
            except (ValueError, TypeError):
                continue

    return dates


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_price(val: float) -> str:
    return f"${val:,.2f}"


def _format_volume(val: float) -> str:
    if val >= 1_000_000:
        return f"{val / 1_000_000:,.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:,.0f}K"
    return f"{val:,.0f}"


def _format_row(label: str, row, last_close: Optional[float] = None) -> str:
    """Format a single day's OHLCV data."""
    parts = [f"{label}: {_format_price(row['Close'])}"]
    parts.append(f"  Open: {_format_price(row['Open'])}, "
                 f"High: {_format_price(row['High'])}, "
                 f"Low: {_format_price(row['Low'])}")
    if row.get("Volume", 0) > 0:
        parts.append(f"  Volume: {_format_volume(row['Volume'])}")
    if last_close is not None:
        change = last_close - row["Close"]
        pct = (change / row["Close"]) * 100
        sign = "+" if change >= 0 else ""
        parts.append(f"  vs current: {sign}{pct:.1f}%")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------

async def fetch_price_context(
    tickers: list[str],
    requested_dates: Optional[list[datetime]] = None,
) -> Optional[str]:
    """Fetch price data for tickers from Yahoo Finance.

    Args:
        tickers: List of ticker symbols to look up.
        requested_dates: Specific dates the user asked about.

    Returns a formatted string to inject into the system prompt.
    """
    if not tickers:
        return None

    tickers = tickers[:3]
    sections: list[str] = []

    for ticker in tickers:
        try:
            data = _fetch_ticker_data(ticker, requested_dates)
            if data:
                sections.append(data)
        except Exception:
            logger.warning("market_data_fetch_failed", ticker=ticker, exc_info=True)

    if not sections:
        return None

    header = "**MARKET DATA (Yahoo Finance):**\n"
    return header + "\n".join(sections)


def _fetch_ticker_data(
    symbol: str,
    requested_dates: Optional[list[datetime]] = None,
) -> Optional[str]:
    """Fetch current + requested historical price data for a ticker."""
    t = yf.Ticker(symbol)

    # Determine how far back we need to go
    now = datetime.now(_ET)
    if requested_dates:
        earliest = min(requested_dates)
        days_back = (now - earliest).days + 30  # buffer
        days_back = max(days_back, 30)
        # yfinance period strings
        if days_back <= 30:
            period = "1mo"
        elif days_back <= 90:
            period = "3mo"
        elif days_back <= 365:
            period = "1y"
        elif days_back <= 730:
            period = "2y"
        elif days_back <= 1825:
            period = "5y"
        else:
            period = "max"
    else:
        period = "1mo"

    hist = t.history(period=period)
    if hist.empty:
        return None

    lines: list[str] = []
    lines.append(f"--- {symbol} ---")

    # Current / last close
    last_row = hist.iloc[-1]
    last_close = last_row["Close"]
    last_date = hist.index[-1].strftime("%B %d, %Y")
    lines.append(_format_row(f"Last close ({last_date})", last_row))

    # Previous close
    if len(hist) >= 2:
        prev_close = hist.iloc[-2]["Close"]
        change = last_close - prev_close
        change_pct = (change / prev_close) * 100
        sign = "+" if change >= 0 else ""
        lines.append(f"  Day change: {sign}{_format_price(change)} ({sign}{change_pct:.2f}%)")

    # 52-week range if we have enough data
    if len(hist) > 100:
        year_hist = hist.tail(252)
        lines.append(f"52-week range: {_format_price(year_hist['Low'].min())} – "
                     f"{_format_price(year_hist['High'].max())}")

    # Requested historical dates — find closest trading day for each
    if requested_dates:
        lines.append("")
        lines.append("Historical lookups:")
        for target in requested_dates:
            idx = hist.index.get_indexer([target], method="nearest")[0]
            if 0 <= idx < len(hist):
                row = hist.iloc[idx]
                actual_date = hist.index[idx].strftime("%B %d, %Y")
                lines.append(_format_row(actual_date, row, last_close))

    # Market cap
    try:
        info = t.fast_info
        mcap = info.market_cap
        if mcap and mcap > 0:
            if mcap >= 1e12:
                lines.append(f"Market cap: ${mcap / 1e12:.2f}T")
            elif mcap >= 1e9:
                lines.append(f"Market cap: ${mcap / 1e9:.1f}B")
            else:
                lines.append(f"Market cap: ${mcap / 1e6:.0f}M")
    except Exception:
        pass

    return "\n".join(lines)
