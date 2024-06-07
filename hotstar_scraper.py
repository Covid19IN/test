
import time
import json
import warnings
import requests
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Process, Manager, Value


def get_tv_shows_list():
    urls = set()
    tv_shows_main_page_url = "https://www.hotstar.com/in/new-sitemap-SHOWS-1.xml"
    response = requests.get(tv_shows_main_page_url, verify=True)
    soup = BeautifulSoup(response.text, "html.parser")
    for i in soup.find_all("loc"):
        urls.add(i.text)
    return list(urls)



def parse_title_info(tv_show_url, result_list, error_list, counter):

    try:
        deep_link_url_prefix = tv_show_url.rsplit("hotstar.com")[-1]

        headers = {
            'authority': 'www.hotstar.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'eng',
            'content-type': 'application/json',
            'origin': 'https://www.hotstar.com',
            'x-hs-platform': 'web',
            'x-hs-usertoken': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBJZCI6IiIsImF1ZCI6InVtX2FjY2VzcyIsImV4cCI6MTcxNzgyNzAwOCwiaWF0IjoxNzE3NzQwNjA4LCJpc3MiOiJUUyIsImp0aSI6IjcwZTJlMDQwZWViNDQzMDRhYjc2ODQzZjg0NWFiYzAyIiwic3ViIjoie1wiaElkXCI6XCI5YzI4OWExYWRlZDU0OGU2OTEzNTU2OTQ4M2U4ODUzOVwiLFwicElkXCI6XCJkMjdjYjgzYjQ0YjY0MGUxOGVhODAxMjBmYTMxOTI1YVwiLFwibmFtZVwiOlwiWW91XCIsXCJpcFwiOlwiMTQuMTkyLjEyLjgyXCIsXCJjb3VudHJ5Q29kZVwiOlwiaW5cIixcImN1c3RvbWVyVHlwZVwiOlwibnVcIixcInR5cGVcIjpcImd1ZXN0XCIsXCJpc0VtYWlsVmVyaWZpZWRcIjpmYWxzZSxcImlzUGhvbmVWZXJpZmllZFwiOmZhbHNlLFwiZGV2aWNlSWRcIjpcIjE3ZTEwMi01OTE5M2QtNTgzZjc0LTMxNTRhMFwiLFwicHJvZmlsZVwiOlwiQURVTFRcIixcInZlcnNpb25cIjpcInYyXCIsXCJzdWJzY3JpcHRpb25zXCI6e1wiaW5cIjp7fX0sXCJpc3N1ZWRBdFwiOjE3MTc3NDA2MDg4NjgsXCJtYXR1cml0eUxldmVsXCI6XCJBXCIsXCJkcGlkXCI6XCJkMjdjYjgzYjQ0YjY0MGUxOGVhODAxMjBmYTMxOTI1YVwiLFwic3RcIjoxLFwiZGF0YVwiOlwiQ2dRSUFCSUFDZ3dJQUNJSWtBSDhnc2FLL3pFS0JBZ0FPZ0FLQkFnQU1nQUtsQUVJQUNxUEFRb0NDZ0FLQkFvQ0NBSUthUW9IQ0FFVkFBQUFRQklLQ2dOb2FXNGxjSXdJUHhJS0NnTjBZVzBsY2JJSFBoSUtDZ04wWld3bEZIWElQUklLQ2dOaVpXNGxRcUtpUFJJS0NnTnRZV3dsR3Y1YlBSSUtDZ050WVhJbGVzaE1QUklLQ2dObGJtY2x3ZTB0UFJJS0NnTnJZVzRsOURBdVBBb0xDZ0lJQXhJRkNnTm9hVzRLQ3dvQ0NBUVNCUW9EYUdsdUNpNElBRUlxQ2loQ1pqZG1aVEZpTTJRellXTTJORGcyTkRoaU1UaGhPRGhqTWpreVpqWXhNbU5XY2xGaE9EWnNcIn0iLCJ2ZXJzaW9uIjoiMV8wIn0.Tt_D5xG1DI3azUODzF30Nu_IKAi7fYZHDEizD0hZq-c',
            'x-request-id': '55858c-56033b-43057f-605d18',
        }

        json_data = {
            'deeplink_url': f'{deep_link_url_prefix}?client_capabilities=%7B%22ads%22%3A%5B%22non_ssai%22%5D%2C%22audio_channel%22%3A%5B%22stereo%22%5D%2C%22container%22%3A%5B%22fmp4%22%2C%22fmp4br%22%2C%22ts%22%5D%2C%22dvr%22%3A%5B%22short%22%5D%2C%22dynamic_range%22%3A%5B%22sdr%22%5D%2C%22encryption%22%3A%5B%22widevine%22%2C%22plain%22%5D%2C%22ladder%22%3A%5B%22web%22%2C%22tv%22%2C%22phone%22%5D%2C%22package%22%3A%5B%22dash%22%2C%22hls%22%5D%2C%22resolution%22%3A%5B%22sd%22%2C%22hd%22%5D%2C%22video_codec%22%3A%5B%22h265%22%2C%22h264%22%5D%2C%22true_resolution%22%3A%5B%22sd%22%2C%22hd%22%2C%22fhd%22%5D%7D&drm_parameters=%7B%22hdcp_version%22%3A%5B%22HDCP_V2_2%22%5D%2C%22widevine_security_level%22%3A%5B%22SW_SECURE_DECODE%22%5D%2C%22playready_security_level%22%3A%5B%5D%7D',
            'app_launch_count': 38,
        }

        response = requests.post(
            'https://www.hotstar.com/api/internal/bff/v2/start', 
            headers=headers, 
            json=json_data,
            verify=False
            )

        result_data = response.json()

        title_name = result_data["success"]["page"]["spaces"]["hero"]["widget_wrappers"][0]["widget"]["data"]["content_info"]["title"]
        description = result_data["success"]["page"]["spaces"]["hero"]["widget_wrappers"][0]["widget"]["data"]["content_info"]["description"]
        title_id = result_data["success"]["page"]["spaces"]["hero"]["widget_wrappers"][0]["widget"]["data"]["content_actions_row"]["content_action_buttons"][0]["watchlist_content_action_button"]["info"]["content_id"]
        title_info_dict = {
            "title_id": title_id,
            "title_name": title_name,
            "description": description,
            "title_page_url": tv_show_url,
        }
        print(f"item_processed: {counter.value+1}\t|\ttitle_id={title_id}, title_name={title_name}")

        result_list.append(title_info_dict)
    except Exception as e:
        print(f"FAILED_TO_PROCESS: {counter.value+1}\t|\terror has occurred for tv_show url: {tv_show_url}. It isn't probably present in india country. error -> {e}")
        error_list.append(tv_show_url)

    with counter.get_lock():
        counter.value += 1



def process_titles_data_batch_worker(batch, result_list, error_list, counter):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        for tv_show_url in batch:
            parse_title_info(tv_show_url, result_list, error_list, counter)


def process_titles_data_batch(tv_shows_url_list, batch_size=6, wait_time=2):
    title_info_list = []
    error_tv_show_urls_list = []

    with Manager() as manager:
        result_list = manager.list()
        error_list = manager.list()
        counter = Value('i', 0)

        batches = [tv_shows_url_list[i:i + batch_size] for i in range(0, len(tv_shows_url_list), batch_size)]

        processes = []

        for i, batch in enumerate(batches, start=1):
            print(f"Processing Batch {i}/{len(batches)}")
            process = Process(target=process_titles_data_batch_worker, args=(batch, result_list, error_list, counter))
            processes.append(process)
            process.start()

            # Sleep for 15 seconds before starting the next process
            new_wait_time = wait_time
            if i%3 == 0:
                new_wait_time = wait_time*3

            print(f"Time to take a break for {new_wait_time} seconds while work is being done in the background. Be patient!")
            time.sleep(new_wait_time)

        for process in processes:
            process.join()

        title_info_list.extend(filter(None, result_list))
        error_tv_show_urls_list.extend(filter(None, error_list))

    return title_info_list, error_tv_show_urls_list



if __name__ == '__main__':

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # main logic
        tv_shows_url_list = get_tv_shows_list()
        # print(tv_shows_url_list[0])

        error_tv_show_urls_list = [
            i for i in tv_shows_url_list if i.rsplit("/")[-1] in ("1260073184", "1260139869", "1260003165") 
        ]

        print(f"########## Total tv_shows info to be scrapped from hotstar: {len(tv_shows_url_list)}")
        title_info_list, error_tv_show_urls_list = process_titles_data_batch(
            tv_shows_url_list=tv_shows_url_list,
            batch_size=76,
            wait_time=40
        )
        print(f"No. of processed titles --> {len(title_info_list)}")
        if title_info_list:
            csv_file = "hotstar_tv_shows_data.csv"
            df = pd.DataFrame(title_info_list)
            print(f"data written in csv file {csv_file} is {len(df)}")
            df.to_csv(csv_file, index=False, encoding='utf-8')
        else:
            print(f"Failed to get title info list for tv_show urls")


        if error_tv_show_urls_list:
            print(f"\n\n########## Retry on failed tv_show_urls. total failed urls: {len(error_tv_show_urls_list)}")
            err_title_info_list, error_tv_show_urls_list = process_titles_data_batch(
                tv_shows_url_list=error_tv_show_urls_list,
                batch_size=20,
                wait_time=10
            )

            if err_title_info_list:
                err_df = pd.DataFrame(err_title_info_list)
                err_csv_file = "err_hotstar_tv_shows_data.csv"
                print(f"data written in errrorenous csv file {err_csv_file} is {len(err_df)}")
                err_df.to_csv(err_csv_file, index=False, encoding='utf-8')
            

            error_json_file = "error_tv_show_urls.json"
            with open(error_json_file, 'w') as f:
                json.dump(error_tv_show_urls_list, f)
            
            print(f"Errored out tv_show urls are written in file {error_json_file}")
        else:
            print("Woohoo!! No failures too!")
