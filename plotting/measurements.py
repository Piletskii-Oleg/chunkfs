import matplotlib.pyplot as plt
import pandas as pd


def create_grouped_bar_charts(csv_filepath):
    try:
        df = pd.read_csv(csv_filepath)
    except FileNotFoundError:
        print(f"Error: file {csv_filepath} not found.")
        return

    df = df[df['chunker'] != "Fixed size chunking, chunk size: 4096"]
    df['chunker'] = df['chunker'].str.split(',').str[0].str.strip()

    metrics = df.columns[df.columns.get_loc('dedup_ratio'):]
    names = df['name'].unique()

    for metric in metrics:
        metric_file = metric
        pivot_df = df.pivot_table(index='name', columns='chunker', values=metric)

        metric = metric.replace("_", " ").title()
        if "Throughput" in metric:
            metric += " (MB/s)"
        elif "Time" in metric:
            metric += " (s)"

        ax = pivot_df.plot(kind='bar', figsize=(12, 6), width=0.8)
        # plt.title(f'{metric}')
        plt.xlabel('Dataset', fontsize=15)
        plt.ylabel(metric, fontsize=15)
        plt.xticks(rotation=0)
        plt.legend()
        plt.tight_layout()
        # plt.grid(True)

        ax.tick_params(axis='y', which='major', labelsize=14)
        ax.tick_params(axis='x', which='major', labelsize=14)

        plt.savefig(f'graph_{metric_file}.png')
        plt.close()

    print("Диаграммы успешно созданы.")


csv_file = "../measurements.csv"
create_grouped_bar_charts(csv_file)
