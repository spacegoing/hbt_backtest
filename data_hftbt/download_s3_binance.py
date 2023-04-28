from datetime import date, timedelta


def generate_urls(start_date, end_date):
  current_date = start_date
  urls = []

  while current_date <= end_date:
    year_month = current_date.strftime('%Y_%m')
    year_month_day = current_date.strftime('%Y-%m-%d')
    url = f"https://eonlabs-orderbook-data.s3.ap-northeast-1.amazonaws.com/BTCUSDT_T_DEPTH_{year_month}/BTCUSDT_T_DEPTH_{year_month_day}.tar.gz"
    urls.append(url)
    current_date += timedelta(days=1)

  return urls


start_date = date(2016, 1, 1)
end_date = date(2024, 1, 1)
urls = generate_urls(start_date, end_date)
