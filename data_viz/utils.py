import re
import pandas as pd
import matplotlib.pyplot as plt

def filter_string(zipcode_string) -> str:

    try:
        pattern = re.compile(r'^\d{4}[A-Z]{2}$')
        
        if type(zipcode_string)==int:
            zipcode_string = str(zipcode_string)
            
        if type(zipcode_string)==str:
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
    colormap = plt.cm.get_cmap('Oranges')  # You can choose a different colormap

    # Create subplots
    fig, ax = plt.subplots(1, 1, figsize=(24, 16))

    # Plot the map in the first subplot
    amsterdam_map.plot(column=target, cmap=colormap, linewidth=0.8, ax=ax, edgecolor='black')  # Specify the edge color

    # Annotate polygons with their numeric values
    for idx, row in amsterdam_map.iterrows():
        ax.annotate(text=str(row[target]), xy=row['geometry'].centroid.coords[0], ha='center', fontsize=8, color='white')

    # Add colorbar to the first subplot
    sm = plt.cm.ScalarMappable(cmap=colormap, norm=plt.Normalize(vmin=amsterdam_map[target].min(), vmax=amsterdam_map[target].max()))
    sm._A = []
    cbar = plt.colorbar(sm, ax=ax)

    # Set plot title for the first subplot
    ax.set_title(title)

    # Return the figure
    return fig