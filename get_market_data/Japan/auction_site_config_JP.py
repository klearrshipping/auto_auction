# Auction sites configuration (URLs). Credentials from Google Secret Manager.
from config.secrets_manager import get_auction_credentials

# Static per-site config (URLs only)
_auction_sites_base = {
    # "AutoPacific": {
    #     "scraping": {
    #         "url": "https://auction.pacificcoastjdm.com/",
    #         "auction_url": "https://auction.pacificcoastjdm.com/auctions/?p=project/searchform&searchtype=max&s&ld",
    #         "sales_data_url": "https://auction.pacificcoastjdm.com/stats/?p=project/searchform&searchtype=max&s&ld",
    #         "sales_search": {
    #             "maker_value": "9",
    #             "model_value": None,
    #             "year_from": "2020",
    #             "year_to": "2025",
    #             "result_value": "1",
    #         }
    #     }
    # },
    "Zervtek": {
        "scraping": {
            "url": "https://auctions.zervtek.com/",
            "auction_url": "https://auctions.zervtek.com/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auctions.zervtek.com/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Manga Auto Import": {
        "scraping": {
            "url": "https://auc.mangaautoimport.ca/",
            "auction_url": "https://auc.mangaautoimport.ca/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auc.mangaautoimport.ca/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Japan Car Auc": {
        "scraping": {
            "url": "https://auc.japancarauc.com/",
            "auction_url": "https://auc.japancarauc.com/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auc.japancarauc.com/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Zen Autoworks": {
        "scraping": {
            "url": "https://auction.zenautoworks.ca/",
            "auction_url": "https://auction.zenautoworks.ca/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auction.zenautoworks.ca/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    }
}

# Merge credentials from Secret Manager into each site's config
_creds = get_auction_credentials()
auction_sites = {
    name: {
        "username": _creds.get(name, {}).get("username", ""),
        "password": _creds.get(name, {}).get("password", ""),
        **cfg
    }
    for name, cfg in _auction_sites_base.items()
}
