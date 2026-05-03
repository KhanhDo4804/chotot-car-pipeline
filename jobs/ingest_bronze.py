import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

LISTING_URL = "https://xe.chotot.com/mua-ban-oto"

STATE_CSV = "/opt/airflow/project/data/bronze/chotot_cars_state.csv"

OUTPUT_CSV = "/opt/airflow/project/data/bronze/chotot_car_data_new.csv"

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def load_existing_state() -> dict:
    if os.path.exists(STATE_CSV):
        df = pd.read_csv(STATE_CSV)
        if "Giá" not in df.columns:
            return dict(zip(df["Link"], [None] * len(df)))
        return dict(zip(df["Link"], df["Giá"]))
    return {}

def crawl_listing_links(existing_state: dict, max_pages: int = 999) -> list[str]:
    links_to_crawl = []
    consecutive_unchanged = 0 
    
    print(f"Loading {len(existing_state)} vehicles from database for CDC comparison.")

    for page_num in range(1, max_pages + 1):
        current_url = f"{LISTING_URL}?page={page_num}"
        print(f"\n--- Scanning page {page_num} ---")

        try:
            response = requests.get(current_url, headers=USER_AGENT, timeout=10)
            response.raise_for_status()
            soup_list = BeautifulSoup(response.content, "html.parser")
            list_ads = soup_list.find_all("a", class_="c15fd2pn")

            if not list_ads:
                print("No more listings.")
                break

            for ad in list_ads:
                href = ad.get("href")
                if not href:
                    continue
                
                detail_link = "https://xe.chotot.com" + href
                price_element = ad.find("span", class_="bfe6oav")
                current_price = price_element.text.strip() if price_element else None

                if detail_link in existing_state:
                    old_price = existing_state[detail_link]
                    if current_price == old_price:
                        consecutive_unchanged += 1
                    else:
                        consecutive_unchanged = 0
                        if detail_link not in links_to_crawl:
                            links_to_crawl.append(detail_link)
                            print(f"   [UPDATE] Vehicle price change: {old_price} -> {current_price}")
                else:
                    consecutive_unchanged = 0
                    if detail_link not in links_to_crawl:
                        links_to_crawl.append(detail_link)
                        print(f"   [NEW] New vehicle found!")
                
                if consecutive_unchanged >= 40:
                    print(f"\n==> Found 40 consecutive vehicles with unchanged prices. Stopping scan!")
                    return links_to_crawl

            time.sleep(1.5)

        except Exception as exc:
            print(f"[!] Error on page {page_num}: {exc}")
            break

    return links_to_crawl

def get_info_by_itemprop(soup: BeautifulSoup, prop_name: str) -> str | None:
    element = soup.find("span", itemprop=prop_name)
    return element.text.strip() if element else None

def crawl_car_details(links_to_crawl: list[str]) -> pd.DataFrame:
    all_cars_details = []

    if not links_to_crawl:
        return pd.DataFrame()

    print(f"\n[*] Starting to extract details for {len(links_to_crawl)} vehicles to process...")

    for i, link in enumerate(links_to_crawl):
        try:
            response = requests.get(link, headers=USER_AGENT, timeout=10)
            soup_detail = BeautifulSoup(response.content, "html.parser")

            car_info = {}
            car_info["Link"] = link

            name = soup_detail.find("span", class_="BreadCrumb_breadcrumbLastItem__Bu4C8")
            car_info["Tên"] = name.text.strip() if name else None
            
            price = soup_detail.find("b", class_="p26z2wb")
            car_info["Giá"] = price.text.strip() if price else None

            addr = soup_detail.find("span", class_="bwq0cbs flex-1")
            car_info["Địa chỉ"] = addr.text.strip() if addr else None
            
            car_info['Số KM đã đi'] = get_info_by_itemprop(soup_detail, 'mileage_v2')
            car_info['Xuất xứ'] = get_info_by_itemprop(soup_detail, 'carorigin')
            car_info['Tình trạng'] = get_info_by_itemprop(soup_detail, 'condition_ad')
            car_info['Hãng'] = get_info_by_itemprop(soup_detail, 'carbrand')
            car_info['Dòng xe'] = get_info_by_itemprop(soup_detail, 'carmodel')
            car_info['Năm sản xuất'] = get_info_by_itemprop(soup_detail, 'mfdate')
            car_info['Hộp số'] = get_info_by_itemprop(soup_detail, 'gearbox')
            car_info['Nhiên liệu'] = get_info_by_itemprop(soup_detail, 'fuel')
            car_info['Kiểu dáng'] = get_info_by_itemprop(soup_detail, 'cartype')
            car_info['Số chỗ'] = get_info_by_itemprop(soup_detail, 'carseats')

            all_cars_details.append(car_info)
            print(f"[{i+1}/{len(links_to_crawl)}] Done: {car_info['Tên']} - {car_info['Giá']}")
            time.sleep(1)

        except Exception as exc:
            print(f"[!] Error on link {link}: {exc}")

    return pd.DataFrame(all_cars_details)

def main():
    existing_state = load_existing_state()

    links_to_crawl = crawl_listing_links(existing_state)

    if links_to_crawl:
        df_new_details = crawl_car_details(links_to_crawl)
        
        df_new_details.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\nSaved {len(df_new_details)} vehicles with details to {OUTPUT_CSV}")
        
        for _, row in df_new_details.iterrows():
            existing_state[row["Link"]] = row["Giá"]
            
        df_updated_state = pd.DataFrame([{"Link": k, "Giá": v} for k, v in existing_state.items()])
        df_updated_state.to_csv(STATE_CSV, index=False, encoding="utf-8-sig")
        print(f"Updated internal state file at {STATE_CSV}")
        
    else:
        print("\nNo new vehicles or price changes found!")

if __name__ == "__main__":
    main()