import os
import re
import zipfile

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import Point


def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")


def extract_zip(zip_file_path, extract_destination):
    # Specify the destination directory where you want to extract the contents

    # Open the zip file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        # Extract all contents to the destination directory
        zip_ref.extractall(extract_destination)

    print(f"Contents extracted to {extract_destination}")


def filter_string(zipcode_string) -> str:
    try:
        pattern = re.compile(r"^\d{4}[A-Z]{2}$")

        if type(zipcode_string) == int:
            zipcode_string = str(zipcode_string)

        if type(zipcode_string) == str:
            zipcode_string = zipcode_string.replace(" ", "")
            if pattern.match(zipcode_string):
                zipcode_string = zipcode_string[:4]

        else:
            zipcode_string = None
    except:
        return None

    return zipcode_string


def create_amsterdam_heatmap(amsterdam_map: pd.DataFrame, target: str, title: str):
    # Create a colormap for zipcodes
    colormap = plt.cm.get_cmap("Oranges")  # You can choose a different colormap

    # Create subplots
    fig, ax = plt.subplots(1, 1, figsize=(24, 16))

    # Plot the map in the first subplot
    amsterdam_map.plot(
        column=target, cmap=colormap, linewidth=0.8, ax=ax, edgecolor="black"
    )  # Specify the edge color

    # Annotate polygons with their numeric values
    for idx, row in amsterdam_map.iterrows():
        ax.annotate(
            text=str(row[target]),
            xy=row["geometry"].centroid.coords[0],
            ha="center",
            fontsize=8,
            color="black",
        )

    # Add colorbar to the first subplot
    sm = plt.cm.ScalarMappable(
        cmap=colormap,
        norm=plt.Normalize(
            vmin=amsterdam_map[target].min(), vmax=amsterdam_map[target].max()
        ),
    )
    sm._A = []
    cbar = plt.colorbar(sm, ax=ax)

    # Set plot title for the first subplot
    ax.set_title(title)

    # Return the figure
    return fig


def extract_values(text):
    # Define a regular expression pattern for extracting numeric values
    pattern = re.compile(r"\b\d+,\d+\b|\b\d+\b")

    # Find all matches in the text
    matches = pattern.findall(text)

    # Convert the matches to integers
    values = [int(match.replace(",", "")) for match in matches]

    return values[0]


def get_zipcode_from_geojson(coordinates, geojson_map):
    latitude, longitude = coordinates
    point = Point(longitude, latitude)

    # Perform a spatial query to find the postal code that contains the given point
    result = geojson_map[geojson_map.geometry.contains(point)]

    # Extract the postal code from the result (assuming it's in a column named 'pc4_code')
    zipcode = result["pc4_code"].values[0] if not result.empty else None

    return zipcode
