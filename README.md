# MLS Standings Tracker

A web application for tracking and displaying the latest Major League Soccer (MLS) standings, built with Python, Streamlit, and Prefect.

## Features

- Real-time MLS standings updates
- Clean, user-friendly interface powered by Streamlit
- Automated data fetching and workflow orchestration with Prefect
- Easily customizable and extendable

## Getting Started

### Prerequisites

- [Python 3.8+](https://www.python.org/)
- [pip](https://pip.pypa.io/)
- [Streamlit](https://streamlit.io/)
- [Prefect](https://www.prefect.io/)

### Installation

```bash
git clone https://github.com/yourusername/mls-standings-tracker.git
cd mls-standings-tracker
pip install -r requirements.txt
```

### Extracting Data

```bash
python src/etl/main.py
```

### Running the App

```bash
streamlit run src/app/main.py
```

### Running Prefect Flows

Refer to the Prefect documentation or the project’s instructions for running and scheduling data workflows.

## Project Structure

```
mls-standings-tracker/
├── src/
|   |── app/
|   |   |── main.py
|   |── etl/
|   |   |── main.py
|   |   |── schemas.py
|   |   |── constants.py
|   |   |── utils.py
|   |   |── main.py
├── requirements.txt
└── README.md
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

Distributed under the [MIT License](LICENSE).

## Acknowledgements

- [MLS Official Website](https://www.mlssoccer.com/)
- [Streamlit](https://streamlit.io/)
- [Prefect](https://www.prefect.io/)
- Other open-source libraries and APIs
