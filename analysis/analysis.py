import pandas as pd

df = pd.read_sql('SELECT sp0_temp, sp1_temp, sp2_temp FROM is1_health', con='sqlite:///is1_health_data.db')

    #   - id: sp0_temp
    #       Conversion coefficients: C0=91.394, C1=-0.089493, C2=3.55e-05, -6.26e-9, 1.89e-13
    #   - id: sp1_temp
    #       Conversion coefficients: C0=91.394, C1=-0.089493, C2=3.55e-05, -6.26e-9, 1.89e-13
    #   - id: sp2_temp
    #       Conversion coefficients: C0=91.394, C1=-0.089493, C2=3.55e-05, -6.26e-9, 1.89e-13
def convert_temp(raw_value):
    C0 = 91.394
    C1 = -0.089493
    C2 = 3.55e-05
    C3 = -6.26e-9
    C4 = 1.89e-13
    temp = (C0 + C1 * raw_value + C2 * raw_value**2 + C3 * raw_value**3 + C4 * raw_value**4)
    return temp

df['sp0_temp_converted'] = df['sp0_temp'].apply(convert_temp)
df['sp1_temp_converted'] = df['sp1_temp'].apply(convert_temp)
df['sp2_temp_converted'] = df['sp2_temp'].apply(convert_temp)

# overall temperature statistics
print("SP0 Temp - Min:", df['sp0_temp_converted'].min(), "Max:", df['sp0_temp_converted'].max(), "Mean:", df['sp0_temp_converted'].mean())
print("SP1 Temp - Min:", df['sp1_temp_converted'].min(), "Max:", df['sp1_temp_converted'].max(), "Mean:", df['sp1_temp_converted'].mean())
print("SP2 Temp - Min:", df['sp2_temp_converted'].min(), "Max:", df['sp2_temp_converted'].max(), "Mean:", df['sp2_temp_converted'].mean())

# plot temperature trends
import matplotlib.pyplot as plt


# calulating arg max and arg min
sp0_max_index = df['sp0_temp_converted'].idxmax()
sp0_min_index = df['sp0_temp_converted'].idxmin()
sp1_max_index = df['sp1_temp_converted'].idxmax()
sp1_min_index = df['sp1_temp_converted'].idxmin()
sp2_max_index = df['sp2_temp_converted'].idxmax()
sp2_min_index = df['sp2_temp_converted'].idxmin()

print("SP0 Max Index:", sp0_max_index, "SP0 Min Index:", sp0_min_index)
print("SP1 Max Index:", sp1_max_index, "SP1 Min Index:", sp1_min_index)
print("SP2 Max Index:", sp2_max_index, "SP2 Min Index:", sp2_min_index)

# corresponding temprature of other two solar panels at sp0 max and min
print("At SP0 Max Index - SP1 Temp:", df.at[sp0_max_index, 'sp1_temp_converted'], "SP2 Temp:", df.at[sp0_max_index, 'sp2_temp_converted'])
print("At SP0 Min Index - SP1 Temp:", df.at[sp0_min_index, 'sp1_temp_converted'], "SP2 Temp:", df.at[sp0_min_index, 'sp2_temp_converted'])
print("At SP1 Max Index - SP0 Temp:", df.at[sp1_max_index, 'sp0_temp_converted'], "SP2 Temp:", df.at[sp1_max_index, 'sp2_temp_converted'])
print("At SP1 Min Index - SP0 Temp:", df.at[sp1_min_index, 'sp0_temp_converted'], "SP2 Temp:", df.at[sp1_min_index, 'sp2_temp_converted'])
print("At SP2 Max Index - SP0 Temp:", df.at[sp2_max_index, 'sp0_temp_converted'], "SP1 Temp:", df.at[sp2_max_index, 'sp1_temp_converted'])
print("At SP2 Min Index - SP0 Temp:", df.at[sp2_min_index, 'sp0_temp_converted'], "SP1 Temp:", df.at[sp2_min_index, 'sp1_temp_converted'])

# Adding the corresponding temperatures for all three solar panels at each index
print("At SP0 Max Index - SP0 Temp:", df.at[sp0_max_index, 'sp0_temp_converted'])
print("At SP0 Min Index - SP0 Temp:", df.at[sp0_min_index, 'sp0_temp_converted'])
print("At SP1 Max Index - SP1 Temp:", df.at[sp1_max_index, 'sp1_temp_converted'])
print("At SP1 Min Index - SP1 Temp:", df.at[sp1_min_index, 'sp1_temp_converted'])
print("At SP2 Max Index - SP2 Temp:", df.at[sp2_max_index, 'sp2_temp_converted'])
print("At SP2 Min Index - SP2 Temp:", df.at[sp2_min_index, 'sp2_temp_converted'])


plt.figure(figsize=(12, 6))
plt.plot(df['sp0_temp_converted'], label='SP0 Temp')
plt.plot(df['sp1_temp_converted'], label='SP1 Temp')
plt.plot(df['sp2_temp_converted'], label='SP2 Temp')
plt.xlabel('Index')
plt.ylabel('Temperature (°C)')
plt.title('Temperature Trends for SP0, SP1, SP2')
plt.legend()
plt.savefig('temperature_trends.png', dpi=300, bbox_inches='tight')
plt.show()



