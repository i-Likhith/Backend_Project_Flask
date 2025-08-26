import requests
import bs4
import json

class AmazonScraper:
    ''' Scrapes Amazon Ecommerce Products and provides data '''

    def scrape_products(self, url):
        '''Scrapes content from the given Amazon URL'''
        try:
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
            r.raise_for_status()
            print(f"Request Successful for URL: {url}")
        except requests.exceptions.RequestException as e:
            print(f'Download/request failed for URL: {url} - {e}')
            return []

        k = bs4.BeautifulSoup(r.content, "html.parser")
        products = k.find_all("div", {"data-component-type": "s-search-result"})
        product_list = []
        for p in products:
            name_tag = p.find("h2")
            name = name_tag["aria-label"].strip() if name_tag and 'aria-label' in name_tag.attrs else "N/A"

            price_whole = p.find("span", class_="a-price-whole")
            price_fraction = p.find("span", class_="a-price-fraction")
            price = "N/A"
            if price_whole:
                price = price_whole.text.strip()
                if price_fraction:
                    price += price_fraction.text.strip()

            rating_tag = p.find("span", class_="a-icon-alt")
            rating = rating_tag.text.strip() if rating_tag else "N/A"

            link_tag = p.find("a", class_="a-link-normal")
            link = f"https://amazon.in{link_tag['href']}" if link_tag and 'href' in link_tag.attrs else "N/A"

            product_list.append({
                "Product Name": name,
                "Price": price,
                "Rating": rating,
                "Link": link
            })
        return product_list

    def save_to_json(self, product_list, filename="products.json"):
        product_json = json.dumps(product_list, indent=4)
        try:
            with open(filename, "w") as json_file:
                json_file.write(product_json)
            print(f"Products details saved in {filename}")
        except IOError as e:
            print(f"Error saving to JSON file {filename}: {e}")

