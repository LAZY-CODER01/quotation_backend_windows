import os
import shutil
import logging
import pythoncom
import requests
from io import BytesIO
from datetime import datetime
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
        
        # Ensure output directory exists - safer cleanup for Docker/Windows to avoid "Device Busy"
        if os.path.exists(self.output_dir):
            for filename in os.listdir(self.output_dir):
                file_path = os.path.join(self.output_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}. Reason: {e}")
                    
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

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"quotation_{gmail_id}_{timestamp}.xlsx"
                output_path = os.path.join(self.output_dir, filename)

                shutil.copy2(self.template_path, output_path)

                if copy_only:
                    return output_path

                extraction_result = extraction_data.get("extraction_result", {})

                # Use EnsureDispatch (early binding) so that Characters().Font
                # is properly resolved via the Excel type library.
                # Thread safety is guaranteed by excel_lock above.
                excel = gencache.EnsureDispatch("Excel.Application")
                excel.Visible = False
                excel.DisplayAlerts = False
                excel.ScreenUpdating = False
                try:
                    excel.Calculation = -4135  # xlCalculationManual
                except Exception:
                    pass

                wb = excel.Workbooks.Open(output_path)
                ws = wb.Worksheets(1)

                self._fill_data(ws, extraction_result, output_path)

                excel.ScreenUpdating = True
                try:
                    excel.Calculation = -4105  # xlCalculationAutomatic
                except Exception:
                    pass

                wb.Save()
                wb.Close(False)
                excel.Quit()

                return output_path

            except Exception as e:
                logger.error(f"Excel Service Error: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return None

            finally:
                if excel:
                    try:
                        excel.Quit()
                    except:
                        pass

                pythoncom.CoUninitialize()

    def _fill_data(self, ws, extraction_result, output_path):
        """
        Fills the Excel data with EXACT formatting, Borders, Colors using win32com.
        """
        requirements = extraction_result.get("Requirements", [])
        
        xlContinuous = 1
        xlThin = 2

        def apply_borders(cell_or_range, col_start=None, col_end=None, target_row=None):
            """Apply per-edge borders reliably via COM."""
            if col_start is not None and col_end is not None and target_row is not None:
                for c in range(col_start, col_end + 1):
                    _c = ws.Cells(target_row, c)
                    for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight]:
                        try:
                            b = _c.Borders(edge)
                            b.LineStyle = xlContinuous
                            b.Weight = xlThin
                            b.Color = color_black
                        except Exception:
                            pass
            else:
                for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight]:
                    try:
                        b = cell_or_range.Borders(edge)
                        b.LineStyle = xlContinuous
                        b.Weight = xlThin
                        b.Color = color_black
                    except Exception:
                        pass
        
        xlCenter = -4108
        xlLeft = -4131
        xlRight = -4152
        xlTop = -4160
        
        xlEdgeLeft = 7
        xlEdgeTop = 8
        xlEdgeBottom = 9
        xlEdgeRight = 10
        
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
        color_yellow_fill = rgb_to_ole("FFFF66")
        color_grey_fill = rgb_to_ole("D9D9D9")
        color_light_green = rgb_to_ole("E2EFDA")
        color_light_blue = rgb_to_ole("DDEBF7")

        # --- COLUMN WIDTHS (match screenshot) ---
        ws.Columns("A:A").ColumnWidth = 6
        ws.Columns("B:B").ColumnWidth = 38
        ws.Columns("C:C").ColumnWidth = 20
        ws.Columns("D:D").ColumnWidth = 22
        ws.Columns("E:E").ColumnWidth = 22
        ws.Columns("F:F").ColumnWidth = 8
        ws.Columns("G:G").ColumnWidth = 8
        ws.Columns("H:H").ColumnWidth = 16
        ws.Columns("I:I").ColumnWidth = 16

        START_ROW = 12
        actual_rows = 0

        tmp_img_dir = os.path.join(self.output_dir, "temp_images")
        os.makedirs(tmp_img_dir, exist_ok=True)

        # --- A. FILL DATA ROWS ---
        for idx, item in enumerate(requirements): 
            row = START_ROW + idx
            actual_rows += 1
            try:
                # --- CHECK FOR SELECTED MATCH ---
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
                        
                # Col 1: SL NO
                cell_sl = ws.Cells(row, 1)
                cell_sl.Value = idx + 1
                cell_sl.Font.Bold = True

                # Col 2: DESCRIPTION (Rich Text)
                # Build the combined text string
                label1 = "Your Requirement:"
                label2 = "We OFFER:"
                full_desc = f"{label1}\n{desc_text}\n\n{label2}\n{offering_text}"
                cell_desc = ws.Cells(row, 2)
                cell_desc.Value = full_desc

                # Step 1: Set whole-cell default font (body text inherits this)
                cell_desc.Font.Name = "Calibri"
                cell_desc.Font.Size = 11
                cell_desc.Font.Bold = False
                cell_desc.Font.Color = color_black

                total_len = len(full_desc)

                # Step 2: Format "Your Requirement:" — purple, bold, underline
                # Use GetCharacters() — works reliably with win32com (unlike Characters())
                lbl1_start = 1
                lbl1_len = len(label1)
                try:
                    if lbl1_len > 0 and lbl1_start + lbl1_len - 1 <= total_len:
                        ch1 = cell_desc.GetCharacters(lbl1_start, lbl1_len).Font
                        ch1.Name = "Calibri"
                        ch1.Size = 13
                        ch1.Bold = True
                        ch1.Underline = 2   # xlUnderlineStyleSingle
                        ch1.Color = color_purple
                except Exception as fmt_err:
                    logger.warning(f"Rich text label1 row {row}: {fmt_err}")

                # Step 3: Format "We OFFER:" — red, bold, underline
                # Starts just after: label1 + \n + desc_text + \n\n  (all 1-indexed)
                lbl2_start = len(label1) + 1 + len(desc_text) + 2 + 1
                lbl2_len = len(label2)
                try:
                    if lbl2_len > 0 and lbl2_start + lbl2_len - 1 <= total_len:
                        ch2 = cell_desc.GetCharacters(lbl2_start, lbl2_len).Font
                        ch2.Name = "Calibri"
                        ch2.Size = 13
                        ch2.Bold = True
                        ch2.Underline = 2   # xlUnderlineStyleSingle
                        ch2.Color = color_red
                except Exception as fmt_err:
                    logger.warning(f"Rich text label2 row {row}: {fmt_err}")

                # Step 4: Bold the body text under each label
                # desc_text starts just after label1 + \n (1-indexed)
                desc_start = len(label1) + 1 + 1   # after "Your Requirement:\n"
                desc_len = len(desc_text)
                try:
                    if desc_len > 0 and desc_start + desc_len - 1 <= total_len:
                        ch_desc = cell_desc.GetCharacters(desc_start, desc_len).Font
                        ch_desc.Bold = True
                        ch_desc.Color = color_black
                        ch_desc.Size = 11
                except Exception as fmt_err:
                    logger.warning(f"Rich text desc body row {row}: {fmt_err}")

                # offering_text starts just after label2 + \n
                offer_start = lbl2_start + lbl2_len + 1
                offer_len = len(offering_text)
                try:
                    if offer_len > 0 and offer_start + offer_len - 1 <= total_len:
                        ch_offer = cell_desc.GetCharacters(offer_start, offer_len).Font
                        ch_offer.Bold = True
                        ch_offer.Color = color_black
                        ch_offer.Size = 11
                except Exception as fmt_err:
                    logger.warning(f"Rich text offer body row {row}: {fmt_err}")

                # Col 3: BRAND (bold)
                cell_brand = ws.Cells(row, 3)
                cell_brand.Value = brand_text
                cell_brand.Font.Bold = True
                cell_brand.Font.Name = "Calibri"
                cell_brand.Font.Size = 11
                
                # Col 4: IMAGE (NEW & CENTERED)
                if selected_match and selected_match.get("image_url"):
                    image_url = selected_match.get("image_url")
                    try:
                        response = requests.get(image_url, timeout=5)
                        if response.status_code == 200:
                            img_data = BytesIO(response.content)
                            pil_img = PilImage.open(img_data).convert("RGBA")
                            
                            canvas_size = (90, 90)
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
                            
                            cell_img = ws.Cells(row, 4)
                            pic_width = 80
                            pic_height = 80
                            pic_left = cell_img.Left + (cell_img.Width - pic_width) / 2
                            pic_top = cell_img.Top + (cell_img.Height - pic_height) / 2
                            
                            ws.Shapes.AddPicture(temp_img_path, False, True, pic_left, pic_top, pic_width, pic_height)
                            
                    except Exception as img_err:
                        logger.error(f"Failed to load/center image for row {row}: {img_err}")

                # Col 5: DELIVERY (RED BOLD)
                cell_del = ws.Cells(row, 5)
                cell_del.Value = "Ex stock, subject to prior sales."
                cell_del.Font.Name = "Calibri"
                cell_del.Font.Size = 11
                cell_del.Font.Color = color_red
                cell_del.Font.Bold = True

                # Col 6: QTY (BOLD)
                qty_val = self._to_float(item.get("Quantity", 0))
                cell_qty = ws.Cells(row, 6)
                cell_qty.Value = qty_val
                cell_qty.NumberFormat = "#,##0.00"
                cell_qty.Font.Name = "Calibri"
                cell_qty.Font.Size = 11
                cell_qty.Font.Bold = True

                # Col 7: UNIT (BOLD)
                cell_unit = ws.Cells(row, 7)
                cell_unit.Value = item.get("Unit", "")
                cell_unit.Font.Name = "Calibri"
                cell_unit.Font.Size = 11
                cell_unit.Font.Bold = True

                # Col 8: UNIT PRICE (BOLD)
                cell_price = ws.Cells(row, 8)
                cell_price.Value = price_val 
                cell_price.NumberFormat = "#,##0.00"
                cell_price.Font.Name = "Calibri"
                cell_price.Font.Size = 11
                cell_price.Font.Bold = True

                # Col 9: TOTAL PRICE FORMULA (BOLD)
                cell_total = ws.Cells(row, 9)
                cell_total.Formula = f"=F{row}*H{row}"
                cell_total.NumberFormat = "#,##0.00"
                cell_total.Font.Name = "Calibri"
                cell_total.Font.Size = 11
                cell_total.Font.Bold = True

                # --- INTERNAL CALCULATIONS (CP/Profit) ---
                
                # Col K (11): CP Input (BOLD)
                cell_cp = ws.Cells(row, 11)
                cell_cp.Value = 0.00
                cell_cp.NumberFormat = "#,##0.00"
                cell_cp.Font.Name = "Calibri"
                cell_cp.Font.Size = 11
                cell_cp.Font.Bold = True

                # Col L (12): % Input (Light Green Background)
                cell_pct = ws.Cells(row, 12)
                cell_pct.Value = 0.00
                cell_pct.NumberFormat = "0.00%"
                cell_pct.Interior.Color = color_light_green

                # Col M (13): Profit Formula (BOLD + Light Blue Background)
                cell_profit = ws.Cells(row, 13)
                cell_profit.Formula = f"=(N{row}-K{row})*F{row}"
                cell_profit.NumberFormat = "#,##0.00"
                cell_profit.Font.Name = "Calibri"
                cell_profit.Font.Size = 11
                cell_profit.Font.Bold = True
                cell_profit.Interior.Color = color_light_blue

                # Col N (14): SP Formula (BOLD)
                cell_sp = ws.Cells(row, 14)
                cell_sp.Formula = f"=K{row}*(1+L{row})"
                cell_sp.NumberFormat = "#,##0.00"
                cell_sp.Font.Name = "Calibri"
                cell_sp.Font.Size = 11
                cell_sp.Font.Bold = True

                # --- APPLY ROW HEIGHT ---
                ws.Rows(row).RowHeight = 140

                # --- APPLY BORDERS & ALIGNMENT (all visible cols 1-9) ---
                for col in range(1, 10):
                    cell = ws.Cells(row, col)

                    # Apply each border edge explicitly – most reliable COM approach
                    for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight]:
                        try:
                            b = cell.Borders(edge)
                            b.LineStyle = xlContinuous
                            b.Weight = xlThin
                            b.Color = color_black
                        except Exception:
                            pass

                    cell.WrapText = True
                    if col == 2:
                        cell.HorizontalAlignment = xlLeft
                        cell.VerticalAlignment = xlTop
                    else:
                        cell.HorizontalAlignment = xlCenter
                        cell.VerticalAlignment = xlCenter

                # Apply borders to internal calc columns 10-14 as well
                for col in range(10, 15):
                    cell = ws.Cells(row, col)
                    for edge in [xlEdgeLeft, xlEdgeTop, xlEdgeBottom, xlEdgeRight]:
                        try:
                            b = cell.Borders(edge)
                            b.LineStyle = xlContinuous
                            b.Weight = xlThin
                            b.Color = color_black
                        except Exception:
                            pass
                    cell.HorizontalAlignment = xlCenter
                    cell.VerticalAlignment = xlCenter
                    cell.WrapText = True

            except Exception as row_error:
                logger.error(f"Error processing row {row}: {row_error}")
                continue

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

            try:
                ws.HPageBreaks.Add(Before=ws.Rows(TERMS_ROW))
            except Exception as e:
                logger.warning(f"Failed to add page break before TERMS: {e}")

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