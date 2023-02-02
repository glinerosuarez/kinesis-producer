import pprint
import inflection

import pandas as pd


df = pd.read_csv("vehicleComponent_20221016_121811.csv")
pp = pprint.PrettyPrinter(indent=4, width=130)
breakpoint()
pp.pprint({c: "string" for c in df.columns})
print()
print("\n".join([f"{inflection.underscore(c)} = df['{c}'] if '{c}' in df.columns else pd.NA" for c in df.columns]))

