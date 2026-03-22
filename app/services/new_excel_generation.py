import os
import shutil
import logging
import pythoncom
import requests
from io import BytesIO
from datetime import datetime
from app.utils.helpers import get_uae_time
from typing import Dict, Optional
from PIL import Image as PilImage
import win32com.client
from win32com.client import gencache
import threading

excel_lock = threading.Lock()
logger = logging.getLogger(__name__)

class ExcelGenerationService:

    def __init__(
        self,
        template_path: str = r"sample/QuotationFormat.xlsx",
        output_dir: str = r"generated",
    ):
        self.template_path = os.path.abspath(template_path)
        self.output_dir = os.path.abspath(output_dir)
        
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Excel Service initialized. Template: {template_path}")

    def generate_quotation_excel(
        self, gmail_id: str, extraction_data: Dict, copy_only: bool = False
    ) -> Optional[str]:
        """
        Generate a quotation Excel file using win32com.
        """
        excel = None
        wb = None
        
        # Thread isolation for Win32COM
        with excel_lock:
            try:
                pythoncom.CoInitialize()
            except Exception as e:
                logger.warning(f"CoInitialize error: {e}")

            try:
                if not os.path.exists(self.template_path):
                    logger.error("Template not found.")
                    return None

                timestamp = get_uae_time().strftime("%Y%m%d_%H%M%S")
                filename = f"quotation_{gmail_id}_{timestamp}.xlsx"
                output_path = os.path.join(self.output_dir, filename)

                shutil.copy2(self.template_path, output_path)

                if copy_only:
                    return output_path

                extraction_result = extraction_data.get("extraction_result", {})

                # Use DispatchEx to ensure a new independent Excel process thread is spawned
                # Thread safety is guaranteed by excel_lock above.
                excel = win32com.client.DispatchEx("Excel.Application")
                excel.Visible = False
                excel.DisplayAlerts = False
                excel.ScreenUpdating = False
                try:
                    excel.Calculation = -4135  # xlCalculationManual
                except Exception:
                    pass

                wb = excel.Workbooks.Open(output_path)
                ws = None
                try:
                    ws = wb.Worksheets(1)
                except:
                    raise Exception("Worksheet initialization failed")

                if ws is None:
                    raise Exception("Worksheet is None")

                self._fill_data(ws, extraction_result, output_path)

                excel.ScreenUpdating = True
                try:
                    excel.Calculation = -4105  # xlCalculationAutomatic
                except Exception:
                    pass

                wb.Save()
                wb.Close(SaveChanges=True)
                excel.Quit()
                del wb
                del excel

                return output_path

            except Exception as e:
                logger.error(f"Excel Service Error: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return None

            finally:
                try:
                    if wb:
                        wb.Close(SaveChanges=True)
                except:
                    pass

                try:
                    if excel:
                        excel.Quit()
                except:
                    pass

                try:
                    del ws
                except:
                    pass

                try:
                    del wb
                except:
                    pass

                try:
                    del excel
                except:
                    pass

                pythoncom.CoUninitialize()

    def _fill_data(self, ws, extraction_result, output_path):
        """
        Fills the Excel data with EXACT formatting, Borders, Colors using win32com.
        """
        requirements = extraction_result.get("Requirements", [])
        if not requirements:
            return
            
        xlContinuous = 1
        xlThin = 2

        xlCenter = -4108
        xlLeft = -4131
        xlRight = -4152
        xlTop = -4160
        
        xlEdgeLeft = 7
        xlEdgeTop = 8
        xlEdgeBottom = 9
        xlEdgeRight = 10
        xlInsideVertical = 11
        xlInsideHorizontal = 12
        
        def apply_borders_to_range(rng):
            """Applies outer and inner borders to an entire grid at once."""
            for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight, xlInsideVertical, xlInsideHorizontal]:
                try:
                    b = rng.Borders(edge)
                    b.LineStyle = xlContinuous
                    b.Weight = xlThin
                    b.Color = color_black
                except Exception:
                    pass

        def apply_borders(cell_or_range, col_start=None, col_end=None, target_row=None):
            """Preserved for footer row compatibility."""
            if col_start is not None and target_row is not None:
                rng = ws.Range(ws.Cells(target_row, col_start), ws.Cells(target_row, col_end))
            else:
                rng = cell_or_range
            for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight]:
                try:
                    b = rng.Borders(edge)
                    b.LineStyle = xlContinuous
                    b.Weight = xlThin
                    b.Color = color_black
                except Exception:
                    pass

        def rgb_to_ole(hex_color):
            hex_color = hex_color.lstrip('#')
            if len(hex_color) == 6:
                r = int(hex_color[0:2], 16)
                g = int(hex_color[2:4], 16)
                b = int(hex_color[4:6], 16)
                return r + (g * 256) + (b * 65536)
            return 0

        color_black = rgb_to_ole("000000")
        color_red = rgb_to_ole("FF0000")
        color_purple = rgb_to_ole("800080")
        color_yellow_fill = rgb_to_ole("#FFFF00")
        color_grey_fill = rgb_to_ole("D9D9D9")
        color_light_green = rgb_to_ole("E2EFDA")
        color_light_blue = rgb_to_ole("DDEBF7")

        START_ROW = 12
        actual_rows = len(requirements)
        last_data_row = START_ROW + actual_rows - 1

        tmp_img_dir = os.path.join(self.output_dir, "temp_images")
        os.makedirs(tmp_img_dir, exist_ok=True)

        # --- A. PARALLEL IMAGE DOWNLOAD ---
        import concurrent.futures
        def download_img(idx, url, row):
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    img_data = BytesIO(response.content)
                    pil_img = PilImage.open(img_data).convert("RGBA")
                    canvas_size = (130, 130)
                    canvas = PilImage.new('RGBA', canvas_size, (255, 255, 255, 0))
                    pil_img.thumbnail(canvas_size, PilImage.LANCZOS)
                    x = (canvas_size[0] - pil_img.width) // 2
                    y = (canvas_size[1] - pil_img.height) // 2
                    canvas.paste(pil_img, (x, y), pil_img)
                    if canvas.mode == "RGBA":
                        bg = PilImage.new('RGB', canvas.size, (255, 255, 255))
                        bg.paste(canvas, mask=canvas.split()[3])
                        canvas = bg
                    temp_img_path = os.path.join(tmp_img_dir, f"img_{row}_{idx}.png")
                    canvas.save(temp_img_path, format='PNG')
                    return row, temp_img_path
            except Exception as e:
                logger.error(f"Image download error row {row}: {e}")
            return row, None

        image_tasks = []
        values_2d = []
        rich_text_data = []

        # --- B. PREPARE ALL ROWS MAPPED TO A SINGLE 2D ARRAY ---
        for idx, item in enumerate(requirements): 
            row = START_ROW + idx
            selected_match = item.get("selectedMatch")
            
            desc_text = str(item.get("Description", "") or "N/A")
            offering_text = str(item.get("Company Offering", "") or "")
            brand_text = item.get("Brand and model", "")
            price_val = self._to_float(item.get("Unit price", 0))
            
            if selected_match:
                if selected_match.get("offer"):
                    offering_text = selected_match.get("offer")
                if selected_match.get("brand"):
                    brand_text = selected_match.get("brand")
                if selected_match.get("price") is not None:
                    price_val = self._to_float(selected_match.get("price"))
                if selected_match.get("image_url"):
                    image_tasks.append((idx, selected_match.get("image_url"), row))
                        
            desc_text = desc_text.replace('\r\n', '\n')
            offering_text = offering_text.replace('\r\n', '\n')

            label1 = "Your Requirement:"
            label2 = "We OFFER:"
            offer_body = offering_text.strip() if offering_text.strip() else " "
            full_desc = f"{label1}\n{desc_text}\n\n{label2}\n{offer_body} "
            
            qty_val = self._to_float(item.get("Quantity", 0))
            unit_val = item.get("Unit", "")
            
            # Map columns A-N (1 to 14)
            values_2d.append([
                idx + 1,                             # 1: SL NO
                full_desc,                           # 2: DESCRIPTION (Rich text handled later)
                brand_text,                          # 3: BRAND
                "",                                  # 4: IMAGE (Placeholder)
                "Ex stock, subject to prior sales.", # 5: DELIVERY
                qty_val,                             # 6: QTY
                unit_val,                            # 7: UNIT
                price_val,                           # 8: PRICE
                f"=F{row}*H{row}",                   # 9: TOTAL Formula
                "",                                  # 10: Empty (border spacer)
                0.00,                                # 11: CP Input
                0.00,                                # 12: % Input
                f"=(N{row}-K{row})*F{row}",          # 13: PROFIT Formula
                f"=K{row}*(1+L{row})"                # 14: SP Formula
            ])
            
            # Store metadata for the rich text pass
            rich_text_data.append((row, len(label1), len(desc_text), len(label2), len(full_desc)))

        image_results = {}
        if image_tasks:
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(download_img, idx, url, row) for idx, url, row in image_tasks]
                    for future in concurrent.futures.as_completed(futures):
                        r, path = future.result()
                        if path:
                            image_results[r] = path
            except Exception as e:
                logger.error(f"Error in parallel image download: {e}")

        # --- C. BULK INSERT & BATCH FORMATTING ---
        # 1 COM call inserts all data and formulas at once!
        data_range = ws.Range(ws.Cells(START_ROW, 1), ws.Cells(last_data_row, 14))
        data_range.Value = values_2d

        # 1 COM call sets the font for the entire grid
        data_range.Font.Name = "Calibri"
        data_range.Font.Size = 11
        data_range.VerticalAlignment = xlCenter
        data_range.HorizontalAlignment = xlCenter
        data_range.WrapText = True

        # Specific Alignment for Description Column
        col_desc = ws.Range(ws.Cells(START_ROW, 2), ws.Cells(last_data_row, 2))
        col_desc.HorizontalAlignment = xlLeft
        col_desc.VerticalAlignment = xlTop

        # Bulk Row Height
        ws.Range(f"A{START_ROW}:A{last_data_row}").RowHeight = 140

        # Bulk Number Formats
        ws.Range(ws.Cells(START_ROW, 6), ws.Cells(last_data_row, 6)).NumberFormat = "#,##0.00"
        ws.Range(ws.Cells(START_ROW, 8), ws.Cells(last_data_row, 9)).NumberFormat = "#,##0.00"
        ws.Range(ws.Cells(START_ROW, 11), ws.Cells(last_data_row, 11)).NumberFormat = "#,##0.00"
        ws.Range(ws.Cells(START_ROW, 12), ws.Cells(last_data_row, 12)).NumberFormat = "0.00%"
        ws.Range(ws.Cells(START_ROW, 13), ws.Cells(last_data_row, 14)).NumberFormat = "#,##0.00"

        # Bulk Colors
        ws.Range(ws.Cells(START_ROW, 5), ws.Cells(last_data_row, 5)).Font.Color = color_red
        ws.Range(ws.Cells(START_ROW, 12), ws.Cells(last_data_row, 12)).Interior.Color = color_light_green
        ws.Range(ws.Cells(START_ROW, 13), ws.Cells(last_data_row, 13)).Interior.Color = color_light_blue

        # Bulk Bold Columns
        for bc in [1, 3, 5, 6, 7, 8, 9, 11, 13, 14]:
            ws.Range(ws.Cells(START_ROW, bc), ws.Cells(last_data_row, bc)).Font.Bold = True

        # Bulk Borders
        apply_borders_to_range(data_range)

        # --- D. APPLY IMAGES & RICH TEXT PER ROW ---
        # This is the only per-row looping we do for COM objects now
        for row, l1_len, desc_len, l2_len, total_len in rich_text_data:
            try:
                # 1. Images
                if row in image_results:
                    temp_img_path = image_results[row]
                    cell_img = ws.Cells(row, 4)
                    pic_width, pic_height = 90, 90
                    pic_left = cell_img.Left + (cell_img.Width - pic_width) / 2
                    pic_top = cell_img.Top + (cell_img.Height - pic_height) / 2
                    ws.Shapes.AddPicture(temp_img_path, False, True, pic_left, pic_top, pic_width, pic_height)
                
                # 2. Rich Text formatting
                cell_desc = ws.Cells(row, 2)
                
                s_A = 1
                l_A = l1_len
                s_B = s_A + l_A
                l_B = 1 + desc_len
                s_C = s_B + l_B
                l_C = 2
                s_D = s_C + l_C
                l_D = l2_len
                s_E = s_D + l_D
                l_E = total_len - s_E + 1

                if l_A > 0 and s_A + l_A - 1 <= total_len:
                    fA = cell_desc.GetCharacters(s_A, l_A).Font
                    fA.Size = 13
                    fA.Bold = True
                    fA.Underline = 2
                    fA.Color = color_purple

                if l_B > 0 and s_B + l_B - 1 <= total_len:
                    fB = cell_desc.GetCharacters(s_B, l_B).Font
                    fB.Size = 11
                    fB.Bold = True
                    fB.Color = color_black
                    fB.Underline = -4142

                if l_C > 0 and s_C + l_C - 1 <= total_len:
                    fC = cell_desc.GetCharacters(s_C, l_C).Font
                    fC.Size = 11
                    fC.Bold = True
                    fC.Color = color_black
                    fC.Underline = -4142

                if l_D > 0 and s_D + l_D - 1 <= total_len:
                    fD = cell_desc.GetCharacters(s_D, l_D).Font
                    fD.Size = 13
                    fD.Bold = True
                    fD.Underline = 2
                    fD.Color = color_red

                if l_E > 0 and s_E + l_E - 1 <= total_len:
                    fE = cell_desc.GetCharacters(s_E, l_E).Font
                    fE.Size = 11
                    fE.Bold = True
                    fE.Color = color_black
                    fE.Underline = -4142
            except Exception as e:
                logger.error(f"Error applying rich text/image for row {row}: {e}")

        # --- B. TOTALS & FOOTER ---
        try:
            last_data_row = START_ROW + max(actual_rows, 1) - 1
            
            TOTAL_ROW = last_data_row + 1
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1
            NOTE_ROW = GRAND_ROW + 1

            # ---- TOTAL AMOUNT ----
            ws.Range(ws.Cells(TOTAL_ROW, 1), ws.Cells(TOTAL_ROW, 8)).Merge()
            ws.Cells(TOTAL_ROW, 1).Value = "Total Amount (AED)."
            ws.Cells(TOTAL_ROW, 9).Formula = f"=SUM(I{START_ROW}:I{last_data_row})"
            
            # ---- VAT ----
            ws.Range(ws.Cells(VAT_ROW, 1), ws.Cells(VAT_ROW, 8)).Merge()
            ws.Cells(VAT_ROW, 1).Value = "VAT 5% (AED)."
            ws.Cells(VAT_ROW, 9).Formula = f"=I{TOTAL_ROW}*0.05"

            # ---- GRAND TOTAL ----
            ws.Range(ws.Cells(GRAND_ROW, 1), ws.Cells(GRAND_ROW, 8)).Merge()
            ws.Cells(GRAND_ROW, 1).Value = "GRAND TOTAL AMOUNT (AED)."
            ws.Cells(GRAND_ROW, 9).Formula = f"=SUM(I{TOTAL_ROW}:I{VAT_ROW})"

            # ---- NOTE ROW ----
            ws.Range(ws.Cells(NOTE_ROW, 1), ws.Cells(NOTE_ROW, 9)).Merge()
            ws.Cells(NOTE_ROW, 1).Value = "NOTE :- STOCK AVAILBILITY AS PER THE QUOTATION DATE KINDLY CONFIRM AT THE TIME OF CONFIRMATION."
            
            # ---- APPLY FORMATTING ----
            
            # 1. Total Amount & VAT (14pt Red Bold)
            for r in (TOTAL_ROW, VAT_ROW):
                ws.Rows(r).RowHeight = 20 
                
                c1 = ws.Cells(r, 1)
                c1.Font.Name = "Calibri"
                c1.Font.Size = 14
                c1.Font.Color = color_red
                c1.Font.Bold = True
                c1.HorizontalAlignment = xlRight
                
                c9 = ws.Cells(r, 9)
                c9.Font.Name = "Calibri"
                c9.Font.Size = 14
                c9.Font.Color = color_red
                c9.Font.Bold = True
                c9.HorizontalAlignment = xlCenter
                c9.NumberFormat = "#,##0.00"
                
                apply_borders(None, 1, 9, r)

            # 2. Grand Total (16pt Red Bold, Yellow Fill)
            ws.Rows(GRAND_ROW).RowHeight = 25 
            
            c1_g = ws.Cells(GRAND_ROW, 1)
            c1_g.Font.Name = "Calibri"
            c1_g.Font.Size = 16
            c1_g.Font.Color = color_red
            c1_g.Font.Bold = True
            c1_g.HorizontalAlignment = xlRight
            
            c9_g = ws.Cells(GRAND_ROW, 9)
            c9_g.Font.Name = "Calibri"
            c9_g.Font.Size = 16
            c9_g.Font.Color = color_red
            c9_g.Font.Bold = True
            c9_g.HorizontalAlignment = xlCenter
            c9_g.NumberFormat = "#,##0.00"
            
            for c in range(1, 10):
                cell = ws.Cells(GRAND_ROW, c)
                cell.Interior.Color = color_yellow_fill
            apply_borders(None, 1, 9, GRAND_ROW)

            # 3. Note Row (Red Bold Note)
            note_cell = ws.Cells(NOTE_ROW, 1)
            note_cell.Font.Name = "Calibri"
            note_cell.Font.Size = 11
            note_cell.Font.Color = color_red
            note_cell.Font.Bold = True
            note_cell.HorizontalAlignment = xlLeft
            note_cell.VerticalAlignment = xlCenter
            
            apply_borders(None, 1, 9, NOTE_ROW)

            # --- TERMS & CONDITIONS ---
            TERMS_ROW = NOTE_ROW + 1 

            ws.Range(ws.Cells(TERMS_ROW, 1), ws.Cells(TERMS_ROW, 9)).Merge()
            t_cell = ws.Cells(TERMS_ROW, 1)
            t_cell.Value = "TERMS:"
            t_cell.Font.Name = "Calibri"
            t_cell.Font.Size = 11
            t_cell.Font.Bold = True
            t_cell.Interior.Color = color_grey_fill
            t_cell.HorizontalAlignment = xlLeft
            
            apply_borders(None, 1, 9, TERMS_ROW)

            # ---- TERMS ROWS ----
            terms_data = [
                ("Price:", "Ex Warehouse Dubai, Packed."),
                ("Delivery:", "Stated in Description Column Against Each Item."),
                ("Payment:", "30 Days Credit."),
                ("Validity:", "15 days from offer date.")
            ]

            current_row = TERMS_ROW
            for i, (label, value) in enumerate(terms_data):
                current_row = TERMS_ROW + 1 + i
                
                c_lbl = ws.Cells(current_row, 1)
                c_lbl.Value = label
                c_lbl.Font.Name = "Calibri"
                c_lbl.Font.Size = 11
                c_lbl.Font.Bold = True
                c_lbl.HorizontalAlignment = xlLeft
                
                ws.Range(ws.Cells(current_row, 2), ws.Cells(current_row, 9)).Merge()
                c_val = ws.Cells(current_row, 2)
                c_val.Value = value
                c_val.Font.Name = "Calibri"
                c_val.Font.Size = 11
                c_val.Font.Bold = True
                c_val.HorizontalAlignment = xlLeft
                
                apply_borders(None, 1, 9, current_row)

            # --- FOOTER MESSAGES & DYNAMIC PRINT AREA ---
            FS_START = current_row + 1
            MSG_ROW = FS_START + 1
            REGARDS_ROW = MSG_ROW + 2
            COMPANY_ROW = REGARDS_ROW + 1
            DISCLAIMER_ROW = COMPANY_ROW + 3
            FS_END = DISCLAIMER_ROW
            
            for r in range(FS_START, FS_END + 1):
                row_range = ws.Range(ws.Cells(r, 1), ws.Cells(r, 9))
                row_range.Merge()
                
                row_range.Borders(xlEdgeLeft).LineStyle = xlContinuous
                row_range.Borders(xlEdgeLeft).Weight = xlThin
                row_range.Borders(xlEdgeRight).LineStyle = xlContinuous
                row_range.Borders(xlEdgeRight).Weight = xlThin
                
                if r == FS_END:
                    row_range.Borders(xlEdgeBottom).LineStyle = xlContinuous
                    row_range.Borders(xlEdgeBottom).Weight = xlThin

            # 1. Message
            msg_cell = ws.Cells(MSG_ROW, 1)
            msg_cell.Value = "Please revert for clarifications if any. Thank you for providing an opportunity to quote."
            msg_cell.HorizontalAlignment = xlLeft
            
            # 2. Best Regards
            reg_cell = ws.Cells(REGARDS_ROW, 1)
            reg_cell.Value = "Best Regards,"
            reg_cell.HorizontalAlignment = xlLeft
            
            # 3. Company Name (Bold)
            comp_cell = ws.Cells(COMPANY_ROW, 1)
            comp_cell.Value = "Dbest Building Hardware and Tools Trading LLC."
            comp_cell.Font.Name = "Calibri"
            comp_cell.Font.Size = 11
            comp_cell.Font.Bold = True
            comp_cell.HorizontalAlignment = xlLeft
        
            # 4. Disclaimer
            disc_cell = ws.Cells(DISCLAIMER_ROW, 1)
            disc_cell.Value = "(This message has been electronically transmitted and does not require a signature)."
            disc_cell.Font.Name = "Calibri"
            disc_cell.Font.Size = 9
            disc_cell.Font.Italic = True
            disc_cell.HorizontalAlignment = xlLeft

            # THE FIX: Extend Blue Line to include template's contact bar images
            FINAL_PRINT_ROW = DISCLAIMER_ROW + 2 

            try:
                ws.PageSetup.PrintArea = f"$A$1:$I${FINAL_PRINT_ROW}"
            except Exception as e:
                logger.warning(f"PrintArea error: {e}")

            # PrintArea controls the print boundary.
            # FitToPage / FitToPagesWide / FitToPagesTall are NOT reliably
            # settable via COM in newer Excel and cause exceptions — removed.

        except Exception as e:
            logger.error(f"Error writing totals/footer: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
        finally:
            try:
                shutil.rmtree(tmp_img_dir, ignore_errors=True)
            except:
                pass

    def _to_float(self, value) -> float:
        try:
            if value is None: return 0.0
            s = str(value).strip().replace(",", "")
            return float(s) if s else 0.0
        except: 
            return 0.0