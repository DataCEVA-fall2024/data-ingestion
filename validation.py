from redfin_scraper import RedfinScraper


scraper = RedfinScraper()
zip_codes = ["53705 "]
scraper.setup("./zip_code_database.csv", multiprocessing=False)

scraper.scrape(city_states=['Omaha, NE'],zip_codes='JUNK')





print(scraper.get_data("D001"))

