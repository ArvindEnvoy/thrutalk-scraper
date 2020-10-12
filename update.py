import gspread_dataframe as gd
import gspread as gs
import pandas as pd

gc = gs.service_account(filename="thrutalk-creds.json")


def export_to_sheets(worksheet_name, df, mode="r"):
    ws = gc.open("test")
    ws.share("arvinddd2003@gmail.com", perm_type="user", role="writer")
    ws = ws.worksheet("Sheet1")
    if mode == "w":
        ws.clear()
        gd.set_with_dataframe(
            worksheet=ws,
            dataframe=df,
            include_index=False,
            include_column_header=True,
            resize=True,
        )
        return True
    elif mode == "a":
        ws.add_rows(df.shape[0])
        import pdb

        pdb.set_trace()
        gd.set_with_dataframe(
            worksheet=ws,
            dataframe=df,
            include_index=False,
            include_column_header=False,
            row=ws.row_count + 1,
            resize=False,
        )
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)


df = pd.DataFrame.from_records([{"a": i, "b": i * 2} for i in range(100)])
ws = gc.open("test")
ws.share("arvinddd2003@gmail.com", perm_type="user", role="writer")
ws = ws.worksheet("Sheet1")
import pdb

pdb.set_trace()
# export_to_sheets("test", df, "a")
