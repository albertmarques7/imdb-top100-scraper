import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

# global headers to be used for requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

MAX_THREADS = 10

def save_to_csv(data):
    """Salva os dados no CSV."""
    with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(data)

def extract_movie_details(movie_link):
    """Extrai título, data, rating e sinopse curta de cada filme."""

    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
    divs = page_section.find_all('div', recursive=False)
    target_div = divs[1]

    # Título
    title_tag = target_div.find('h1')
    title = title_tag.get_text().strip() if title_tag else None

    # Data de lançamento
    date_tag = movie_soup.find('a', href=lambda href: href and 'releaseinfo' in href)
    date = date_tag.get_text().strip() if date_tag else None

    # Rating (nota IMDB)
    rating_tag = movie_soup.find('span', class_='ipc-rating-star--rating')
    rating = rating_tag.get_text().strip() if rating_tag else None

    # Sinopse curta
    plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
    plot_text = plot_tag.get_text().strip() if plot_tag else None

    if all([title, date, rating, plot_text]):
        print([title, date, rating, plot_text])
        save_to_csv([title, date, rating, plot_text])

def extract_movies(soup):

    """Extrai links dos 100 filmes e roda as threads."""
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/pt/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()
