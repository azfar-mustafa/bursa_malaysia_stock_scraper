from BursaScraper import main
import requests

def test_bursa_scraper_status_code_equals_200():
    response = requests.get("https://bursascraper.azurewebsites.net/api/bursascraper?name=functions")
    assert response.status_code == 200