import win32com.client
import os

excel = win32com.client.DispatchEx("Excel.Application")
excel.Visible = False
wb = excel.Workbooks.Add()
ws = wb.Worksheets(1)

def rgb_to_ole(hex_color):
    hex_color = hex_color.lstrip('#')
    return int(hex_color[0:2], 16) + (int(hex_color[2:4], 16) * 256) + (int(hex_color[4:6], 16) * 65536)

color_black = rgb_to_ole("000000")
color_purple = rgb_to_ole("800080")
color_red = rgb_to_ole("FF0000")

label1 = "Your Requirement:"
label2 = "We OFFER:"
desc_text = "Some description"
offering_text = ""

full_desc = f"{label1}\n{desc_text}\n\n{label2}\n{offering_text}"

cell = ws.Cells(1, 1)
cell.Value = full_desc

cell.Font.Color = color_black
cell.Font.Underline = -4142

ch1 = cell.GetCharacters(1, len(label1)).Font
ch1.Color = color_purple
ch1.Underline = 2

lbl2_start = len(label1) + 1 + len(desc_text) + 2 + 1
ch2 = cell.GetCharacters(lbl2_start, len(label2)).Font
ch2.Color = color_red
ch2.Underline = 2

if offering_text:
    offer_start = lbl2_start + len(label2) + 1
    ch3 = cell.GetCharacters(offer_start, len(offering_text)).Font
    ch3.Color = color_black
    ch3.Underline = -4142

out_path = os.path.abspath("test_output.xlsx")
if os.path.exists(out_path):
    os.remove(out_path)
wb.SaveAs(out_path)
wb.Close()
excel.Quit()
print("Saved to", out_path)
