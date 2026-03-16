import json
import time
import urllib.parse
from typing import Any

import aiohttp
import structlog

logger = structlog.get_logger()

def convert_list_to_api(list_url: str, page: int = 1, page_size: int = 300) -> str:
    base_api = "https://m.kbchachacha.com/public/web/search/infinitySearch.json"
    
    parsed = urllib.parse.urlparse(list_url)
    original_params = dict(urllib.parse.parse_qsl(parsed.query))
    
    # Убираем фрагмент (#...), если он есть в URL, часто параметры дублируются там
    if parsed.fragment:
        # Простая эвристика, если параметры в фрагменте
        pass 

    api_params = {
        "sort": "-paymentPlayYn,-orderDate",
        "page": str(page),
        "pageSize": str(page_size),
        "includeFields": (
            "carSeq,fileNameArray,ownerYn,makerName,className,carName,"
            "modelName,gradeName,regiDay,yymm,km,cityCodeName2,"
            "sellAmtGbn,sellAmt,sellAmtPrev,carMasterSpecialYn,"
            "monthLeaseAmt,interestFreeYn,ownerYn,directYn,"
            "carAccidentNo,warrantyYn,falsityYn,kbCertifiedYn" 
            # Сократил поля для краткости, добавьте остальные при необходимости
        ),
        "displaySoldoutYn": "Y",
        "paymentPremiumYn": "Y",
        "searchAfter": "",
        "v": str(int(time.time() * 1000))
    }
    
    final_params = {**original_params, **api_params}
    return f"{base_api}?{urllib.parse.urlencode(final_params)}"

# Список всех полей, которые вам нужны
FIELDS = [
    "carSeq",
    "fileNameArray",
    "ownerYn",
    "makerName",
    "className",
    "carName",
    "modelName",
    "gradeName",
    "regiDay",
    "yymm",
    "km",
    "cityCodeName2",
    "sellAmtGbn",
    "sellAmt",
    "sellAmtPrev",
    "carMasterSpecialYn",
    "monthLeaseAmt",
    "interestFreeYn",
    "directYn",
    "carAccidentNo",
    "warrantyYn",
    "falsityYn",
    "kbLeaseYn",
    "friendDealerYn",
    "orderDate",
    "certifiedShopYn",
    "kbCertifiedYn",
    "hasOverThreeFileNames",
    "diagYn",
    "diagGbn",
    "lineAdYn",
    "tbMemberMemberName",
    "colorCodeName",
    "gasName",
    "safeTel",
    "carHistorySeq",
    "homeserviceYn2",
    "labsDanjiNo2",
    "paymentPremiumYn",
    "paymentPremiumText",
    "paymentPremiumMarkNmArray",
    "paymentPlayYn",
]


async def fetch_cars(
    session: aiohttp.ClientSession, 
    api_url: str, 
    max_pages: int = 10
) -> dict[str, Any] | None:    
    parsed_url = urllib.parse.urlparse(api_url)
    base_api = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

    # Preserve original filters from the stored URL, but avoid duplicating pagination/sort params.
    original_pairs = urllib.parse.parse_qsl(parsed_url.query, keep_blank_values=True)
    filtered_pairs: list[tuple[str, str]] = [
        (k, v)
        for k, v in original_pairs
        if k not in {"v", "searchAfter", "includeFields", "page", "pageSize", "sort"}
    ]

    accumulated_hits: list[dict[str, Any]] = []
    current_cursor: list[Any] | None = None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; Mobile) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        ),
        "Referer": "https://m.kbchachacha.com/",
        "Accept": "application/json",
    }

    max_pages = 10

    for page_num in range(max_pages):
        params: list[tuple[str, str]] = []
        params.extend(filtered_pairs)
        params.extend(
            [
                ("sort", "-paymentPlayYn,-orderDate"),
                ("pageSize", "300"),
                ("page", "1"),
                ("displaySoldoutYn", "Y"),
                ("paymentPremiumYn", "Y"),
                ("v", str(int(time.time() * 1000))),
            ]
        )

        for field in FIELDS:
            params.append(("includeFields", field))

        if current_cursor:
            for val in current_cursor:
                params.append(("searchAfter", str(val)))

        logger.info("Fetching page", page=page_num + 1, search_after=current_cursor or [])

        async with session.get(base_api, params=params, headers=headers) as resp:
            if resp.status != 200:
                logger.error("KB API Error", status=resp.status, url=str(resp.url))
                return None

            data = await resp.json()

        result = data.get("result") if isinstance(data, dict) else None
        hits = result.get("hits") if isinstance(result, dict) else None
        if not isinstance(hits, list) or not hits:
            break

        accumulated_hits.extend(hits)

        next_cursor = result.get("searchAfter") if isinstance(result, dict) else None
        if not isinstance(next_cursor, list) or not next_cursor:
            break
        current_cursor = next_cursor

    if not isinstance(data, dict):
        return None

    # Normalize shape for downstream code
    data["list"] = accumulated_hits
    if isinstance(data.get("result"), dict):
        data["result"]["hits"] = accumulated_hits
    return data