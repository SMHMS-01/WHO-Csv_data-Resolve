import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import mapclassify
from shapely.geometry import Polygon, MultiPolygon
from matplotlib.collections import PatchCollection
from matplotlib.widgets import Button

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

plt.rcParams['font.family'] = 'Microsoft Yahei'
plt.rcParams['font.size'] = 10

fig, ax = plt.subplots(1, 1, figsize=(10, 6))
ax.set_position([0.05, 0.1, 0.8, 0.8])
ax.set_aspect('equal')

fig.suptitle(
    "The Global COVID-19 Situation  Source: World Heath Organization", fontweight='bold')
ax.set_ylabel('Longitude', fontsize=15, color='black')
ax.set_xlabel('Latitude', fontsize=15, color='black')

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_linestyle('dashed')
ax.spines['left'].set_linestyle('dashed')


gdf = gpd.read_file('E:/PY/output/output.shp')
gdf.crs = 'epsg:4326'


collection_text = {}


def set_annotates(ax, patch_collection, _text, xy, visible):
    _anno = ax.annotate(_text, xy, xytext=(0, -10),
                        textcoords="offset points", ha='center', va='center', color='black', size=15,
                        bbox=dict(boxstyle="round", facecolor='white', alpha=0.5))
    _anno.set_visible(visible)
    collection_text[patch_collection] = _anno


def geom_polygon_to_patch(geom_polygon):
    try:
        coordinates = np.array(geom_polygon.exterior.coords)
        patch = plt.Polygon(coordinates)
        return patch
    except TypeError:
        raise TypeError(
            f"Parameter type error: Your parameter type is '{type(geom_polygon)}' ---> 'shapely.geometry.multipolygon.MultiPolygon'")


def geom_multipolygon_to_collection(geom_multipolygon):
    try:
        list_patches = []
        for geom_polygon in geom_multipolygon.geoms:
            list_patches.append(geom_polygon_to_patch(geom_polygon))
        collection = PatchCollection(list_patches)
        return collection
    except TypeError:
        raise TypeError(
            f"Parameter type error: Your parameter type is '{type(geom_multipolygon)}' ---> 'shapely.geometry.multipolygon.MultiPolygon'")


def axes_graphies_settings(patch_or_collection, is_visible=False, is_picker=False):
    patch_or_collection.set_visible(is_visible)
    patch_or_collection.set_edgecolor('black')
    patch_or_collection.set_facecolor('none')
    patch_or_collection.set_linewidth(0.2)
    # patch_or_collection.set_alpha(1)
    patch_or_collection.set_picker(is_picker)


global_patch_or_collection_with_annotate = {}


def set_axes_graphies_annotate(ax, patch_or_collection, text, xy_coordinations, is_visible=False):
    annotate = ax.annotate(text, xy_coordinations, xytext=(0, -10),
                           textcoords="offset points", ha='center',
                           va='center', color='black', size=7.5,
                           bbox=dict(boxstyle="round", facecolor='white', alpha=0.7))
    annotate.set_visible(is_visible)
    global_patch_or_collection_with_annotate[patch_or_collection] = annotate


def add_graphies_to_axes(ax, geodataframe):
    for _, _row in geodataframe.iterrows():

        geometry_data = _row.geometry
        text = f"{_row['FORMAL_EN']} : {_row['Cases']}"
        xy_centroid = geometry_data.centroid.coords[0]

        if isinstance(geometry_data, MultiPolygon):
            collection = geom_multipolygon_to_collection(geometry_data)
            axes_graphies_settings(
                patch_or_collection=collection, is_visible=True, is_picker=True)
            ax.add_collection(collection)
            set_axes_graphies_annotate(
                ax=ax, patch_or_collection=collection, text=text, xy_coordinations=xy_centroid, is_visible=False)

        elif isinstance(geometry_data, Polygon):
            patch = geom_polygon_to_patch(geometry_data)
            axes_graphies_settings(
                patch_or_collection=patch, is_visible=True, is_picker=True)
            ax.add_patch(patch)
            set_axes_graphies_annotate(
                ax=ax, patch_or_collection=patch, text=text, xy_coordinations=xy_centroid, is_visible=False)


pick_event_preArtist = None
pick_event_preFacecolor = None


def on_pick(event):
    global pick_event_preArtist, pick_event_preFacecolor, global_patch_or_collection_with_annotate
    if event.artist in global_patch_or_collection_with_annotate and \
            global_patch_or_collection_with_annotate[event.artist].get_visible() == False:
        pick_event_preFacecolor = event.artist.get_facecolor()
        event.artist.set_facecolor('red')
        global_patch_or_collection_with_annotate[event.artist].set_visible(
            True)
        fig.canvas.draw()
    else:
        event.artist.set_facecolor(pick_event_preFacecolor)
        global_patch_or_collection_with_annotate[event.artist].set_visible(
            False)
        fig.canvas.draw()
    pick_event_preArtist = event.artist


fig.canvas.mpl_connect('pick_event', on_pick)


def preload_Choropleth(ax, geodataframe, legend_kwds=None):

    breaks = [100000, 1000000, 5000000, 10000000,
              20000000, 30000000, 50000000, 100000000, geodataframe['Cases'].max()]

    classifier = mapclassify.UserDefined(geodataframe['Cases'], bins=breaks)

    legend_kwds = {'loc': 'center left', 'bbox_to_anchor': (1, 0.3),
                   'title': 'COVID-19 确诊病例世界地图', 'fmt': "{:.0f}"}
    geodataframe.plot(ax=ax, column='Cases', scheme=classifier.name,
                      edgecolor='black', linewidth=0.2,
                      cmap='YlOrRd', legend=True,
                      classification_kwds={"bins": classifier.bins},
                      legend_kwds=legend_kwds,
                      categorical=True,
                      alpha=0.65
                      )
    ax.collections[-1].set_visible(False)
    ax.get_legend().set_visible(False)


global_is_called_Choropleth = False


def on_button_clicked_Choropleth(event):
    global global_is_called_Choropleth, gdf, ax
    if not global_is_called_Choropleth:
        ax.collections[-1].set_visible(True)
        ax.get_legend().set_visible(True)
        fig.canvas.draw()
        global_is_called_Choropleth = True
    else:
        ax.collections[-1].set_visible(False)
        ax.get_legend().set_visible(False)
        fig.canvas.draw()
        global_is_called_Choropleth = False


ax_button_0 = plt.axes([0.1, 0.05, 0.1, 0.05])
button_0 = Button(ax_button_0, '分级统计图',
                  color='#F8A05E', hovercolor='#ED774A')
button_0.on_clicked(on_button_clicked_Choropleth)


if __name__ == '__main__':
    add_graphies_to_axes(ax, gdf)
    preload_Choropleth(ax, gdf)

    plt.show()
