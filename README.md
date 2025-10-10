Repository to cure metadata for citrus and sugarcane

## CITRUS - RNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `citrus`
    2. Filters: RNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Citrus"[Organism] OR citrus[All Fields]) AND ("biomol rna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## CITRUS - DNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `citrus`
    2. Filters: DNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Citrus"[Organism] OR citrus[All Fields]) AND ("biomol dna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## SACCHARUM - RNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `saccharum`
    2. Filters: RNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Saccharum"[Organism] OR Saccharum[All Fields]) AND ("biomol rna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

## SACCHARUM - DNA
1. https://www.ncbi.nlm.nih.gov/
    1. SRA (sequence read archive) -> `saccharum`
    2. Filters: DNA; paired; Illumina; fastQ; all organisms
    3. Run selector - download all
    ```
    ("Saccharum"[Organism] OR Saccharum[All Fields]) AND ("biomol dna"[Properties] AND "library layout paired"[Properties] AND "platform illumina"[Properties] AND "filetype fastq"[Properties])
    ```

---
### Usage
> [!CAUTION]
> Unstable version. Use with caution the scripts.

To run the script, we strongly recommend using **`uv`**.  
If you don’t have `uv` installed, you can find installation instructions here: [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)

1. `get_all_info.py`: used to retrieve information about SRA, particularly whether a PubMed ID is associated with the BioProject. This tool utilizes the NCBI Entrez API.

Parameters:  
- `email`: email to use the NCBI Entrez API
- `input`: the `.csv` file downloaded before in the NCBI _Run Selector_
- `output`: name of the `.csv` output.

```python
uv run get_all_info.py --email email@test.com --input your_csv_file.csv --output your_file_output.csv
```

2. `get_scholar.py`: used to retrieve information from Google Scholar for each BioProject, saving by default the first five results. Utilizes the Firefox WebDriver.

Parameters:  
- `input`: the `.csv` after the `get_all_info.py` execution
- `output`: name of the `.csv` output.

```python
uv run get_all_info.py --input your_csv_file.csv --output your_file_output.csv
```

3. `get_doi_from_url.py`: used to extract the DOI from the URL of each article.

Parameters:  
- `input`: the `.csv` after the `get_scholar.py` execution
- `json_dir`: the folder that contains the `json` files obtained from the execution of `get_scholar.py`.

```python
uv run get_doi_from_url.py --input your_csv_file.csv --json_dir json_directory
```

4. `load_dna.ipynb` and `load_rna.ipynb`: used for initial exploratory analysis and to extract basic information from the table obtained through NCBI _Run Selector_. You only need to update the `.csv` file path and execute each cell.

5. `get_pdf_from_json.py`: used to download the PDF of each article using the DOI. It also extracts the text from the PDF and saves it in a `.txt` file.
> [!NOTE]
> To run this script, you need to have API KEY for `springer-nature`, `wiley`, and `elsevier`. You can get them for free by creating an account on their respective websites.
> The format of the API keys file should be as follows:
```json
{
    "springer-nature": "your_springer_api_key",
    "wiley": "your_wiley_api_key",
    "elsevier": "your_elsevier_api_key"
}
```
> For `wiley`, you need to `export` the API key as an environment variable:
```bash
export TDM_API_TOKEN=your_wiley_api_key
```

There are a API rate limiter implemented to avoid hitting the API limits. The default limit is set to 450 requests per source.

Parameters:
- `input`: the json folder that contains the 'json' files where the file names are the BioProject IDs with DOIs.
- `apikeys`: the `.json` file that contains the API keys for `springer-nature`, `wiley`, and `elsevier`.
- `email`: email to use the APIs

```python
uv run get_pdf_from_json.py --input your_csv_file.csv --apikeys your_api_keys.json --email email@test.com
```

6. `filter_data.ipynb`: used to filter the dataframes to keep only BioProjects with at least six runs. You only need to update the `.csv` file path and execute each cell.


