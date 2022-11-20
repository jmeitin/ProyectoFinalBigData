import hltv

df = hltv.get_results_url("urls", range(1))

match = hltv.MatchPageParams(df)
match.add_all_params()

maps = hltv.LastMaps(df)
maps.add_all_params()

df.to_csv("data.csv")



