import os
import json
import pandas as pd
import matplotlib.pyplot as plt


def get_avg():
    month_max_avg_temp = {}
    months = ["January", "February", "March"]

    # Loop through partition numbers 0-3
    for i in range(4):
        path = f"files/partition-{i}.json"
        if os.path.exists(path):
            with open(path, "r") as file:
                partition_data = json.load(file)

                for month in months:
                    if month in partition_data:

                        # retrieve the year and the agg_stats from the data we collected from 
                        # partition-i
                        for year, agg_stats in partition_data[month].items():
                            if agg_stats["avg"] > 0:
                                month_max_avg_temp[month] = {
                                    "year": year,
                                    "avg": agg_stats["avg"]
                                }
    #print(f"Plot this: {month_max_avg_temp}")
    return month_max_avg_temp

def main():

    # we will plot this data
    max_temp_avg_data = get_avg()
    
    month_series = pd.Series({
        f"{month}-{max_temp_avg_data[month]['year']}": max_temp_avg_data[month]['avg']
        for month in max_temp_avg_data
    })
    fig, ax = plt.subplots()
    month_series.plot.bar(ax=ax)
    ax.set_ylabel('Avg. Max Temperature')
    plt.tight_layout()
    plt.savefig("/files/month.svg")
    
if __name__ == "__main__":
    main()
