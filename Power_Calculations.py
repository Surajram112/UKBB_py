import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.stats.power import NormalIndPower

# Constants
GP_cases = 8793
HES_cases = 2998
N_GP = 209832
N_HES = 451229
alpha = 1e-9
ORs = [1.5, 2, 3]
MAFs = np.arange(0.001, 0.031, 0.001)

# Function to calculate power
def calculate_power(N, case_rate, MAF, OR, alpha):
    power_analysis = NormalIndPower()
    effect_size = np.log(OR) / np.sqrt(2 * MAF * (1 - MAF))
    power = power_analysis.solve_power(effect_size=effect_size, nobs1=N * case_rate, alpha=alpha, ratio=(1 - case_rate) / case_rate, alternative='two-sided')
    return power

# Calculate power for GP records
gp_power_data = []
for OR in ORs:
    for MAF in MAFs:
        power = calculate_power(N_GP, GP_cases / N_GP, MAF, OR, alpha)
        gp_power_data.append({'MAF': MAF, 'OR': OR, 'Power': power, 'Source': 'GP'})

gp_power_df = pd.DataFrame(gp_power_data)

# Calculate power for HES records
hes_power_data = []
for OR in ORs:
    for MAF in MAFs:
        power = calculate_power(N_HES, HES_cases / N_HES, MAF, OR, alpha)
        hes_power_data.append({'MAF': MAF, 'OR': OR, 'Power': power, 'Source': 'HES'})

hes_power_df = pd.DataFrame(hes_power_data)

# Combine data
all_power_df = pd.concat([gp_power_df, hes_power_df])

# Plotting
sns.set(style="whitegrid")
HGtheme = {
    'axes.edgecolor': 'black',
    'axes.grid': False,
    'axes.facecolor': 'white',
    'axes.titlepad': 20,
    'axes.titlesize': 15,
    'axes.titleweight': 'bold',
    'axes.labelsize': 12,
    'axes.labelweight': 'bold',
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'legend.title_fontsize': 12,
    'figure.figsize': (10, 4)
}

plt.rcParams.update(HGtheme)

# Plot GP power
gp_plot = sns.relplot(data=gp_power_df, x='MAF', y='Power', hue='OR', kind='line', palette='muted')
gp_plot.set_axis_labels('MAF', 'Power')
gp_plot.set_titles('Power in GP Records')
gp_plot.savefig("GP_power.png", dpi=300)

# Plot HES power
hes_plot = sns.relplot(data=hes_power_df, x='MAF', y='Power', hue='OR', kind='line', palette='muted')
hes_plot.set_axis_labels('MAF', 'Power')
hes_plot.set_titles('Power in HES Records')
hes_plot.savefig("HES_power.png", dpi=300)

# Combined plot
combined_plot = sns.relplot(data=all_power_df, x='MAF', y='Power', hue='Source', style='OR', kind='line', palette='muted')
combined_plot.set_axis_labels('MAF (%)', 'Power')
combined_plot.set_titles('Power by MAF and OR')
combined_plot.savefig("All_power.png", dpi=300)

plt.show()
