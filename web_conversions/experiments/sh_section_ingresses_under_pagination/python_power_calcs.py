!pip install google-cloud-bigquery pandas numpy scipy statsmodels db-dtypes pyarrow
!pip install google-cloud-bigquery-storage


from google.cloud import bigquery
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import ttest_ind, ttest_ind_from_stats
from statsmodels.stats.power import TTestIndPower
import math
from statsmodels.stats.proportion import proportions_ztest


# Set billing
client = bigquery.Client(project = 'etsy-bigquery-adhoc-prod')

sql = """
select 
  if(variant_id = 'off', 'Control', 'Treatment') as test_group
  , bucketing_id 
  , case when conversion_rate > 0 then 1 else 0 end as converted
  , coalesce(winsorized_acbv,0) as winsorized_acbv
from `etsy-data-warehouse-dev.madelinecollins.xp_section_ingress_desktop_python`
;"""
query_job = client.query(sql) # Run query
results = query_job.result()
df_exp = results.to_dataframe() # Save results to dataframe


df_exp.head()

print(df_exp.dtypes)


def ab_test_analysis(df):
    metrics = {}
    
    # Group data by test group
    treatment = df[df['test_group'] == 'Treatment']
    control = df[df['test_group'] == 'Control']
    
    def compute_lift_and_ci(treatment_mean, control_mean, df_col):
        lift = (treatment_mean - control_mean) / control_mean * 100
        se = stats.sem(df_col)
        ci_low, ci_high = stats.t.interval(0.95, len(df)-1, loc=lift, scale=se * 100)
        return lift, ci_low, ci_high
    
    def compute_power(treatment_col, control_col, col_name):
        effect_size = (treatment_col.mean() - control_col.mean()) / np.std(df[col_name])
        power = TTestIndPower().solve_power(effect_size=effect_size,
                                            nobs1=len(treatment_col),
                                            ratio=len(control_col) / len(treatment_col),
                                            alpha=0.05)
        return power
    
    # Conversion Rate (Two-sided t-test)
    conv_rate_treatment = treatment['converted'].mean()
    conv_rate_control = control['converted'].mean()
    t_stat, p_val = stats.ttest_ind(treatment['converted'], control['converted'])
    power = compute_power(treatment['converted'], control['converted'], 'converted')
    lift, ci_low, ci_high = compute_lift_and_ci(conv_rate_treatment, conv_rate_control, df['converted'])
    metrics['Conversion Rate'] = [conv_rate_treatment, conv_rate_control, lift, p_val, power, ci_low, ci_high]

    
    # Average Converting User Value (Welch’s t-test)
    acuv_treatment = treatment[treatment['converted'] == 1]['winsorized_acbv'].mean()
    acuv_control = control[control['converted'] == 1]['winsorized_acbv'].mean()
    t_stat, p_val = stats.ttest_ind(treatment[treatment['converted'] == 1]['winsorized_acbv'],
                                    control[control['converted'] == 1]['winsorized_acbv'], equal_var=False)
    power = compute_power(treatment[treatment['converted'] == 1]['winsorized_acbv'], control[control['converted'] == 1]['winsorized_acbv'], 'winsorized_acbv')
    lift, ci_low, ci_high = compute_lift_and_ci(acuv_treatment, acuv_control, df['winsorized_acbv'])
    metrics['Avg Converting User Value'] = [acuv_treatment, acuv_control, lift, p_val, power, ci_low, ci_high]
    
    #  # Average Order Value (Welch’s t-test)
    # if treatment['n_orders'].sum() > 0 and control['n_orders'].sum() > 0:
    #     aov_treatment = treatment['gms_wins'].sum() / treatment['n_orders'].sum()
    #     aov_control = control['gms_wins'].sum() / control['n_orders'].sum()
    #     t_stat, p_val = stats.ttest_ind(treatment['gms_wins'] / treatment['n_orders'],
    #                                     control['gms_wins'] / control['n_orders'], equal_var=False, nan_policy='omit')
    #     power = compute_power(treatment['gms_wins'] / treatment['n_orders'], control['gms_wins'] / control['n_orders'], 'gms_wins')
    #     lift, ci_low, ci_high = compute_lift_and_ci(aov_treatment, aov_control, df['gms_wins'] / df['n_orders'])
    # else:
    #     aov_treatment, aov_control, lift, p_val, power, ci_low, ci_high = [np.nan] * 7 # ensures that metrics are filled with NaN if there's insufficient data
    # metrics['Avg Order Value'] = [aov_treatment, aov_control, lift, p_val, power, ci_low, ci_high]
    
    # # GMS Per User (Welch’s t-test)
    # gms_per_user_treatment = treatment['gms_wins'].sum() / len(treatment)
    # gms_per_user_control = control['gms_wins'].sum() / len(control)
    # t_stat, p_val = stats.ttest_ind(treatment['gms_wins'], control['gms_wins'], equal_var=False)
    # power = compute_power(treatment['gms_wins'], control['gms_wins'], 'gms_wins')
    # lift, ci_low, ci_high = compute_lift_and_ci(gms_per_user_treatment, gms_per_user_control, df['gms_wins'])
    # metrics['GMS Per User'] = [gms_per_user_treatment, gms_per_user_control, lift, p_val, power, ci_low, ci_high]
    
    # # Orders Per User (Welch’s t-test)
    # orders_per_user_treatment = treatment['n_orders'].sum() / len(treatment)
    # orders_per_user_control = control['n_orders'].sum() / len(control)
    # t_stat, p_val = stats.ttest_ind(treatment['n_orders'], control['n_orders'], equal_var=False)
    # power = compute_power(treatment['n_orders'], control['n_orders'], 'n_orders')
    # lift, ci_low, ci_high = compute_lift_and_ci(orders_per_user_treatment, orders_per_user_control, df['n_orders'])
    # metrics['Orders Per User'] = [orders_per_user_treatment, orders_per_user_control, lift, p_val, power, ci_low, ci_high]
    
    # Convert to DataFrame
    results_df = pd.DataFrame.from_dict(metrics, orient='index',
                                        columns=['Treatment Mean', 'Control Mean', 'Lift %', 'p-value', 'Power', 'CI Lower', 'CI Upper'])
    
    return results_df




results = ab_test_analysis(df_exp)
print(results)
