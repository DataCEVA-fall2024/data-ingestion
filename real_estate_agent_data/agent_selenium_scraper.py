import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Path to your ChromeDriver
CHROMEDRIVER_PATH = '/usr/local/share/chromedriver'

# Initialize Selenium WebDriver
chrome_options = Options()
chrome_options.add_argument('--headless')  # Run headless Chrome
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
custom_user_agent = "User-Agent Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
referer = "https://www.redfin.com/real-estate-agents"
cookie = "RF_BROWSER_ID=W82c9glRSi--1ngCZeAsVQ; RF_CORVAIR_LAST_VERSION=547.2.0; RF_BID_UPDATED=1; RF_VISITED=true; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+04+2024+17%3A13%3A33+GMT-0600+(Central+Standard+Time)&version=202403.1.0&hosts=&groups=C0001%3A1%2CC0003%3A1%2CSPD_BG%3A1%2CC0002%3A1%2CC0004%3A1&consentId=1c6a2b9a-ca0a-410b-9b4c-7a47047202fd; RF_BROWSER_CAPABILITIES=%7B%22screen-size%22%3A4%2C%22events-touch%22%3Afalse%2C%22ios-app-store%22%3Afalse%2C%22google-play-store%22%3Afalse%2C%22ios-web-view%22%3…52C%2520CO%252C%2520USA%26url%3D%252Fcity%252F5155%252FCO%252FDenver%252Freal-estate%252Fagents%26id%3D10_5155%26type%3D10%26unifiedSearchType%3D10%26isSavedSearch%3D%26countryCode%3DUS; aws-waf-token=b167e0f0-5d11-4f46-b49b-f68a1945dd3f:EgoAdhsNVZS1AwAA:H6JJpsnuDyxr16c396sqWcmi76G6gguIYwCWd+Fl8mOunJAMuBR2EAIk5/Z1me9aBtMG18VtMfAacSgaquAzLD0TXW8AjT8prU6sT/SZELAwDrTRYRMfklBWwAUVXGL62Ih6Dd1RXcWe0s6x2yvzkU5ZZ2aj0Sm9vwbEC57yBBtC5e/f3xzAhyx5U6lEXS4j0Udi+aYLdsgI5zi5oMCTni4qdE4TAtXMpTwbF5B3DSkYt13cdvAtqkFQlz0T2g=="
chrome_options.add_argument(f"--referer={referer}")
chrome_options.add_argument(f"--user-agent={custom_user_agent}")
chrome_options.add_argument(f"--cookie={cookie}")
service = ChromeService(executable_path=CHROMEDRIVER_PATH)
driver = webdriver.Chrome(options=chrome_options)

try:
    print("HELLO")
    url = 'https://www.redfin.com/city/5155/CO/Denver/real-estate/agents'

    driver.get(url)

    wait = WebDriverWait(driver, 10)

    count = 0
    # Wait for the "See more agents" button
    while count < 5:
        try:
            see_more_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.CollapsingControl button')))
            see_more_button.click()
            # Wait for new agents to load
            time.sleep(2)
            count += 1
        except Exception as e:
            print("No more 'See more agents' button found.")
            break
        

    # Get the updated page source
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    print(soup)

    # Find all agent cards
    agents_div = soup.find('div', class_='agents')
    print(agents_div)
    agent_cards = agents_div.find_all('div', class_='RegionalAgentCard')

    number_of_agents = len(agent_cards)
    print(f"Total number of agents in Denver: {number_of_agents}")

finally:
    driver.quit()

