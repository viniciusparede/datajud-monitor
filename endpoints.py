import requests
from bs4 import BeautifulSoup
import pandas as pd


def extract_data(url: str, max_redirects=10) -> bytes:
    """
    Extracts data from a given URL.

    Parameters
    ----------
    url : str
        The URL from which data will be extracted.
    max_redirects : int, optional
        Maximum number of redirects allowed. Defaults to 10.

    Returns
    -------
    bytes
        The content of the page in bytes.
    """

    session = requests.Session()
    session.max_redirects = max_redirects

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }

    try:
        response = session.get(url, headers=headers)

        if response.status_code == 200:
            return response.content
        else:
            print(
                f"Failed to retrieve data from the URL. Status Code: {response.status_code}"
            )

    except requests.exceptions.TooManyRedirects:
        print("Error: TooManyRedirects - Exceeded the maximum number of redirects.")


def transform_data(extracted_data: bytes) -> pd.DataFrame:
    """
    Transforms extracted HTML data into a NumPy array.

    Parameters
    ----------
    extracted_data : bytes
        The HTML content of the page in bytes.

    Returns
    -------
    pd.DataFrame
        Transformed data in a pandas DataFrame.
    """
    soup = BeautifulSoup(extracted_data, "html.parser")

    tables = soup.find_all("table")

    dfs = []

    for table in tables:
        columns = [th.text.upper() for th in table.find_all("th")]
        data = [
            [td.text for td in row.find_all("td")] for row in table.find_all("tr")[1:]
        ]

        dfs.append(pd.DataFrame(data, columns=columns))

    return pd.concat(dfs, ignore_index=True)


def get_endpoints() -> pd.DataFrame:
    url = "https://datajud-wiki.cnj.jus.br/api-publica/endpoints/"

    # Data Extraction
    extracted_data = extract_data(url)

    # Data Transformation to pandas DataFrame
    transformed_dataframe = transform_data(extracted_data)

    return transformed_dataframe





if __name__ == "__main__":
    endpoints = get_endpoints()
    print(endpoints)
