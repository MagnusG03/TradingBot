use crate::utils::normalize_ticker;

struct SectorEntry {
    sector: &'static str,
    benchmark: &'static str,
    tickers: &'static [&'static str],
}

const SECTOR_ENTRIES: &[SectorEntry] = &[
    SectorEntry {
        sector: "Consumer Electronics",
        benchmark: "XLK",
        tickers: &["AAPL"],
    },
    SectorEntry {
        sector: "Software",
        benchmark: "XLK",
        tickers: &[
            "MSFT", "CRM", "ORCL", "ADBE", "NOW", "PLTR", "SNOW", "PANW", "CRWD", "ZS", "NET",
            "MDB", "DDOG",
        ],
    },
    SectorEntry {
        sector: "Internet Services",
        benchmark: "XLC",
        tickers: &["GOOG", "GOOGL"],
    },
    SectorEntry {
        sector: "E-Commerce and Cloud",
        benchmark: "XLY",
        tickers: &["AMZN"],
    },
    SectorEntry {
        sector: "Social Media",
        benchmark: "XLC",
        tickers: &["META"],
    },
    SectorEntry {
        sector: "Streaming Media",
        benchmark: "XLC",
        tickers: &["NFLX"],
    },
    SectorEntry {
        sector: "Electric Vehicles",
        benchmark: "XLY",
        tickers: &["TSLA"],
    },
    SectorEntry {
        sector: "Semiconductors",
        benchmark: "SOXX",
        tickers: &[
            "NVDA", "AMD", "INTC", "QCOM", "AVGO", "MU", "TXN", "ARM", "ASML", "MRVL",
        ],
    },
    SectorEntry {
        sector: "Computer Hardware",
        benchmark: "XLK",
        tickers: &["SMCI", "DELL", "HPQ"],
    },
    SectorEntry {
        sector: "IT Services",
        benchmark: "XLK",
        tickers: &["IBM", "ACN"],
    },
    SectorEntry {
        sector: "Ride Sharing",
        benchmark: "XLY",
        tickers: &["UBER", "LYFT"],
    },
    SectorEntry {
        sector: "Travel Services",
        benchmark: "XLY",
        tickers: &["ABNB", "BKNG", "EXPE"],
    },
    SectorEntry {
        sector: "Banks",
        benchmark: "XLF",
        tickers: &["JPM", "BAC", "WFC", "C"],
    },
    SectorEntry {
        sector: "Asset Management",
        benchmark: "XLF",
        tickers: &["GS", "MS", "SCHW", "BLK"],
    },
    SectorEntry {
        sector: "Payment Processing",
        benchmark: "XLF",
        tickers: &["V", "MA", "AXP", "PYPL", "SQ", "XYZ"],
    },
    SectorEntry {
        sector: "Brokerage and Crypto Services",
        benchmark: "XLF",
        tickers: &["COIN", "HOOD"],
    },
    SectorEntry {
        sector: "Bitcoin Treasury",
        benchmark: "IBIT",
        tickers: &["MSTR"],
    },
    SectorEntry {
        sector: "Bitcoin Mining",
        benchmark: "IBIT",
        tickers: &["MARA", "RIOT", "CLSK"],
    },
    SectorEntry {
        sector: "Diversified Financial Services",
        benchmark: "XLF",
        tickers: &["BRK.B", "BRK-B"],
    },
    SectorEntry {
        sector: "Weight Loss and Diabetes Treatments",
        benchmark: "XLV",
        tickers: &["LLY", "NVO"],
    },
    SectorEntry {
        sector: "Pharmaceuticals",
        benchmark: "XLV",
        tickers: &["JNJ", "PFE", "MRK", "BMY", "ABBV", "GILD"],
    },
    SectorEntry {
        sector: "Biotechnology",
        benchmark: "XLV",
        tickers: &["AMGN", "REGN", "VRTX", "BIIB", "MRNA"],
    },
    SectorEntry {
        sector: "Managed Care",
        benchmark: "XLV",
        tickers: &["UNH", "CI", "HUM", "CVS"],
    },
    SectorEntry {
        sector: "Medical Devices",
        benchmark: "XLV",
        tickers: &["ISRG", "SYK", "MDT", "BSX"],
    },
    SectorEntry {
        sector: "Retail",
        benchmark: "XLY",
        tickers: &["WMT", "COST", "TGT", "DG", "DLTR"],
    },
    SectorEntry {
        sector: "Home Improvement Retail",
        benchmark: "XLY",
        tickers: &["HD", "LOW"],
    },
    SectorEntry {
        sector: "Apparel",
        benchmark: "XLY",
        tickers: &["NKE", "LULU"],
    },
    SectorEntry {
        sector: "Restaurants",
        benchmark: "XLY",
        tickers: &["SBUX", "MCD", "CMG", "YUM", "DPZ"],
    },
    SectorEntry {
        sector: "Beverages",
        benchmark: "XLP",
        tickers: &["KO", "PEP", "MNST", "KDP"],
    },
    SectorEntry {
        sector: "Household Products",
        benchmark: "XLP",
        tickers: &["PG", "CL", "KMB"],
    },
    SectorEntry {
        sector: "Media and Entertainment",
        benchmark: "XLY",
        tickers: &["DIS", "ROKU", "PARA", "WBD"],
    },
    SectorEntry {
        sector: "Cruise Lines",
        benchmark: "XLY",
        tickers: &["RCL", "CCL", "NCLH"],
    },
    SectorEntry {
        sector: "Hotels",
        benchmark: "XLY",
        tickers: &["MAR", "HLT"],
    },
    SectorEntry {
        sector: "Oil and Gas",
        benchmark: "XLE",
        tickers: &["XOM", "CVX", "COP", "BP", "SHEL", "OXY"],
    },
    SectorEntry {
        sector: "Oilfield Services",
        benchmark: "XLE",
        tickers: &["SLB", "HAL", "BKR"],
    },
    SectorEntry {
        sector: "Heavy Machinery",
        benchmark: "XLI",
        tickers: &["CAT", "DE", "PCAR"],
    },
    SectorEntry {
        sector: "Aerospace and Defense",
        benchmark: "XLI",
        tickers: &["BA", "LMT", "NOC", "RTX", "GD"],
    },
    SectorEntry {
        sector: "Industrial Equipment",
        benchmark: "XLI",
        tickers: &["GE", "GEV", "HON", "ETN", "EMR"],
    },
    SectorEntry {
        sector: "Transportation and Logistics",
        benchmark: "XLI",
        tickers: &["UPS", "FDX", "UNP", "CSX", "NSC"],
    },
    SectorEntry {
        sector: "Steel",
        benchmark: "XLB",
        tickers: &["NUE", "X", "STLD"],
    },
    SectorEntry {
        sector: "Copper Mining",
        benchmark: "XLB",
        tickers: &["FCX", "SCCO"],
    },
    SectorEntry {
        sector: "Gold Mining",
        benchmark: "XLB",
        tickers: &["NEM", "GOLD"],
    },
    SectorEntry {
        sector: "Telecommunications",
        benchmark: "XLC",
        tickers: &["T", "VZ", "TMUS", "CMCSA", "CHTR"],
    },
    SectorEntry {
        sector: "Utilities",
        benchmark: "XLU",
        tickers: &["NEE", "DUK", "SO", "D", "AEP", "EXC"],
    },
    SectorEntry {
        sector: "Cell Towers",
        benchmark: "XLRE",
        tickers: &["AMT", "CCI"],
    },
    SectorEntry {
        sector: "Real Estate",
        benchmark: "XLRE",
        tickers: &["PLD", "SPG", "O", "DLR", "EQIX"],
    },
    SectorEntry {
        sector: "Broad Market ETF",
        benchmark: "SPY",
        tickers: &["SPY", "VOO", "IVV"],
    },
    SectorEntry {
        sector: "Nasdaq 100 ETF",
        benchmark: "QQQ",
        tickers: &["QQQ", "TQQQ", "SQQQ"],
    },
    SectorEntry {
        sector: "Dow Jones ETF",
        benchmark: "DIA",
        tickers: &["DIA"],
    },
    SectorEntry {
        sector: "Small-Cap ETF",
        benchmark: "IWM",
        tickers: &["IWM"],
    },
    SectorEntry {
        sector: "Semiconductor ETF",
        benchmark: "SOXX",
        tickers: &["SMH", "SOXX", "SOXL", "SOXS"],
    },
    SectorEntry {
        sector: "Financials ETF",
        benchmark: "XLF",
        tickers: &["XLF"],
    },
    SectorEntry {
        sector: "Technology ETF",
        benchmark: "XLK",
        tickers: &["XLK"],
    },
    SectorEntry {
        sector: "Energy ETF",
        benchmark: "XLE",
        tickers: &["XLE"],
    },
    SectorEntry {
        sector: "Industrials ETF",
        benchmark: "XLI",
        tickers: &["XLI"],
    },
    SectorEntry {
        sector: "Healthcare ETF",
        benchmark: "XLV",
        tickers: &["XLV"],
    },
    SectorEntry {
        sector: "Consumer Staples ETF",
        benchmark: "XLP",
        tickers: &["XLP"],
    },
    SectorEntry {
        sector: "Consumer Discretionary ETF",
        benchmark: "XLY",
        tickers: &["XLY"],
    },
    SectorEntry {
        sector: "Materials ETF",
        benchmark: "XLB",
        tickers: &["XLB"],
    },
    SectorEntry {
        sector: "Utilities ETF",
        benchmark: "XLU",
        tickers: &["XLU"],
    },
    SectorEntry {
        sector: "Real Estate ETF",
        benchmark: "XLRE",
        tickers: &["XLRE", "VNQ"],
    },
    SectorEntry {
        sector: "Disruptive Innovation ETF",
        benchmark: "ARKK",
        tickers: &["ARKK"],
    },
    SectorEntry {
        sector: "Bitcoin ETF",
        benchmark: "IBIT",
        tickers: &["BITO", "IBIT", "FBTC"],
    },
];

pub fn lookup_sector(ticker: &str) -> Option<String> {
    find_sector_entry(&normalize_ticker(ticker)).map(|entry| entry.sector.to_string())
}

pub fn lookup_sector_benchmark_symbol(ticker: &str, sector: Option<&str>) -> Option<&'static str> {
    let ticker = normalize_ticker(ticker);
    find_sector_entry(&ticker)
        .map(|entry| entry.benchmark)
        .or_else(|| sector.and_then(benchmark_for_sector))
}

pub fn build_ticker_news_url(ticker: &str) -> String {
    format!(
        "https://news.google.com/rss/search?q={}+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen",
        normalize_ticker(ticker)
    )
}

pub fn build_market_news_url() -> &'static str {
    "https://news.google.com/rss/search?q=%22US+markets%22+OR+%22Wall+Street%22+OR+finance+OR+economy+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen"
}

pub fn build_sector_news_url(sector: &str) -> String {
    let encoded = sector.split_whitespace().collect::<Vec<_>>().join("+");

    format!(
        "https://news.google.com/rss/search?q=%22{0}%22+OR+%22{0}+sector%22+OR+%22{0}+stocks%22+OR+%22{0}+industry%22+when%3A1d&hl=en-US&gl=US&ceid=US%3Aen",
        encoded
    )
}

fn find_sector_entry(ticker: &str) -> Option<&'static SectorEntry> {
    SECTOR_ENTRIES.iter().find(|entry| {
        entry
            .tickers
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(ticker))
    })
}

fn benchmark_for_sector(sector: &str) -> Option<&'static str> {
    SECTOR_ENTRIES
        .iter()
        .find(|entry| entry.sector.eq_ignore_ascii_case(sector))
        .map(|entry| entry.benchmark)
}
