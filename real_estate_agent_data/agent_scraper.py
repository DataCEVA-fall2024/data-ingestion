import requests
from bs4 import BeautifulSoup
import time

def get_total_agents(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    total_agents = 0
    while True:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        agents_div = soup.find('div', class_='agents')
        if not agents_div:
            break
        agent_cards = agents_div.find_all('div', class_='RegionalAgentCard')
        print(soup.select('RegionalAgentCard'))
        total_agents += len(agent_cards)
        
        # Find the 'Next' button to navigate to the next page
        next_button = soup.find('a', {'rel': 'next'})
        if next_button and 'href' in next_button.attrs:
            next_page_url = 'https://www.redfin.com' + next_button['href']
            url = next_page_url
            time.sleep(1)  # Pause between requests to be respectful
        else:
            break
    return total_agents

url = 'https://www.redfin.com/city/5155/CO/Denver/real-estate/agents'
total_agents = get_total_agents(url)
print(f"Total number of agents in Denver: {total_agents}")

