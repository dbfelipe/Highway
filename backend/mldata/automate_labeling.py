import pandas as pd

# Load the dataset
file_path = "mlchurches_to_label.csv"  # Update with your file path
df = pd.read_csv(file_path)

# Define keyword-based church type mapping
church_type_keywords = {
    "Baptist": "Baptist",
    "Methodist": "Methodist",
    "Pentecostal": "Pentecostal",
    "Lutheran": "Lutheran",
    "Presbyterian": "Presbyterian",
    "Episcopal": "Episcopal",
    "Evangelical": "Evangelical",
    "Catholic": "Catholic",
    "Orthodox": "Orthodox",
    "Non-Denominational": "Non-Denominational",
    "Church of Christ": "Church of Christ",
    "Assemblies of God": "Pentecostal",
    "Seventh-day Adventist": "Seventh-day Adventist"
}
# Function to auto-assign church type
def assign_church_type(name):
    for keyword, church_type in church_type_keywords.items():
        if keyword.lower() in str(name).lower():
            return church_type
    return "Needs Review"  # Mark if no match is found

# Apply function to assign types
df["Auto-Assigned Type"] = df["name"].apply(assign_church_type)

# Standardize Worship Styles
worship_style_corrections = {
    "Contmeporary": "Contemporary",
    "Gregorian": "Hymnal-Based",
}

df["Worship-Style"] = df["Worship-Style"].replace(worship_style_corrections)

# Identify mismatches (where assigned type differs from existing label)
df["Mismatch"] = df.apply(lambda row: row["Auto-Assigned Type"] != row["Type of Church"], axis=1)

# Save the cleaned and labeled dataset
output_file_path = "labeled_churches.csv"
df.to_csv(output_file_path, index=False)

print(f"Labeled data saved to {output_file_path}. Review mismatches manually.")
