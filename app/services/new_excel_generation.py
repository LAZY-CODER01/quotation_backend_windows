import os
import logging
import shutil
from datetime import datetime
from typing import Dict, Optional

# --- Import OpenPyXL (Works on Linux/Render) ---
try:
    from openpyxl import load_workbook
    from openpyxl.cell.rich_text import TextBlock, CellRichText
    from openpyxl.cell.text import InlineFont
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
except ImportError:
    raise ImportError("CRITICAL: 'openpyxl' is missing. Add 'openpyxl>=3.1.0' to requirements.txt")

logger = logging.getLogger(__name__)

class ExcelGenerationService:
    """
    Service class for generating quotation Excel files.
    Optimized for Linux/Render (Docker) environments.
    """

    def __init__(
        self,
        template_path: str = "sample/QuotationFormat.xlsx",
        output_dir: str = "generated",
    ):
        self.template_path = template_path
        self.output_dir = output_dir

        # Ensure output directory exists
        if os.path.exists(output_dir):
            try:
                shutil.rmtree(output_dir)
            except Exception as e:
                logger.error(f"Failed to clean output dir: {e}")
        
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Excel Service initialized. Template: {template_path}")

    def generate_quotation_excel(
        self, gmail_id: str, extraction_data: Dict, copy_only: bool = False
    ) -> Optional[str]:
        """
        Generate a quotation Excel file using OpenPyXL (Render Compatible).
        """
        try:
            # 1. Validation
            if not os.path.exists(self.template_path):
                logger.error(f"❌ Template not found at: {self.template_path}")
                return None

            # 2. Setup Filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quotation_{gmail_id}_{timestamp}.xlsx"
            output_path = os.path.join(self.output_dir, filename)

            # 3. Copy Template
            shutil.copy2(self.template_path, output_path)

            if copy_only:
                return output_path

            # 4. Fill Data
            extraction_result = extraction_data.get("extraction_result", {})
            
            # Using OpenPyXL to edit the file
            wb = load_workbook(output_path)
            ws = wb.active
            
            self._fill_data(ws, extraction_result)
            
            wb.save(output_path)
            wb.close()
            
            logger.info(f"✅ Excel generated successfully: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error generating Excel: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _fill_data(self, ws, extraction_result):
        """
        Fills the Excel data with EXACT formatting, Borders, and Colors.
        """
        requirements = extraction_result.get("Requirements", [])
        
        # --- STYLING CONSTANTS ---
        # 1. Borders: Thin Black & Thick Black
        thin_side = Side(border_style="thin", color="000000")
        full_border = Border(top=thin_side, bottom=thin_side, left=thin_side, right=thin_side)
        
        thick_side = Side(border_style="medium", color="000000")
        thick_border = Border(top=thin_side, bottom=thin_side, left=thin_side, right=thin_side)
        
        # 2. Alignments
        center_align = Alignment(horizontal='center', vertical='center', wrap_text=True)
        left_top_align = Alignment(horizontal='left', vertical='top', wrap_text=True)
        right_align = Alignment(horizontal="right", vertical="center")
        left_center_align = Alignment(horizontal="left", vertical="center")
        
        # 3. Fonts
        red_font = Font(name='Calibri', size=11, color="FF0000")
        red_bold = Font(bold=True, color="FF0000", name="Calibri", size=12)
        red_bold_big = Font(bold=True, color="FF0000", name="Calibri", size=13)
        bold_font = Font(bold=True, name='Calibri', size=11)
        italic_small = Font(italic=True, size=9, name='Calibri')
        
        # 4. Fills
        yellow_fill = PatternFill(fill_type="solid", fgColor="FFFF66")
        grey_fill = PatternFill(fill_type="solid", fgColor="D9D9D9")
        
        START_ROW = 12
   
        actual_rows = 0

        # --- A. FILL DATA ROWS ---
        for idx, item in enumerate(requirements):
            row = START_ROW + idx
            actual_rows += 1
            try:
                # Col 1: SL NO
                ws.cell(row=row, column=1).value = idx + 1

                # Col 2: DESCRIPTION (Rich Text)
                desc_text = str(item.get("Description", "") or "N/A")
                offering_text = str(item.get("Company Offering", "") or "")

                # Rich Text Construction
                # Header 1: "Your Requirement:" (Purple, Bold)
                header1 = TextBlock(InlineFont(b=True, u="single", color="800080"), "Your Requirement:\n")
                body1 = TextBlock(InlineFont(color="000000"), f"{desc_text}\n\n")
                
                # Header 2: "We OFFER:" (Red, Bold)
                header2 = TextBlock(InlineFont(b=True, u="single", color="FF0000"), "We OFFER:\n")
                body2 = TextBlock(InlineFont(color="000000"), f"{offering_text}")

                cell_desc = ws.cell(row=row, column=2)
                cell_desc.value = CellRichText([header1, body1, header2, body2])

                # Col 3: BRAND
                ws.cell(row=row, column=3).value = item.get("Brand and model", "")

                # Col 5: DELIVERY (RED TEXT)
                red_bold_font = Font(name='Calibri', size=11, color="FF0000", bold=True)
                cell_del = ws.cell(row=row, column=5, value="Ex stock, subject to prior sales.")
                cell_del.font = red_bold_font

                # Col 6: QTY
                qty_val = self._to_float(item.get("Quantity", 0))
                ws.cell(row=row, column=6).value = qty_val
                ws.cell(row=row, column=6).number_format = "#,##0.00"

                # Col 7: UNIT
                ws.cell(row=row, column=7).value = item.get("Unit", "")

                # Col 8: UNIT PRICE (Client View)
                price_val = self._to_float(item.get("Unit price", 0))
                ws.cell(row=row, column=8).value = price_val 
                ws.cell(row=row, column=8).number_format = "#,##0.00"

                # Col 9: TOTAL PRICE FORMULA
                # Formula: Qty * Unit Price
                ws.cell(row=row, column=9).value = f"=F{row}*H{row}"
                ws.cell(row=row, column=9).number_format = "#,##0.00"
                
                # --- INTERNAL CALCULATIONS (CP/Profit) ---
                # Col K (11): CP Input (Default 0)
                ws.cell(row=row, column=11).value = 0.00
                ws.cell(row=row, column=11).number_format = "#,##0.00"

                # Col L (12): % Input (Default 0)
                ws.cell(row=row, column=12).value = 0.00
                ws.cell(row=row, column=12).number_format = "0.00%"

                # Col N (14): SP Formula = CP * (1 + %)
                ws.cell(row=row, column=14).value = f"=K{row}*(1+L{row})"
                ws.cell(row=row, column=14).number_format = "#,##0.00"

                # Col M (13): Profit Formula = (SP - CP) * Qty
                ws.cell(row=row, column=13).value = f"=(N{row}-K{row})*F{row}"
                ws.cell(row=row, column=13).number_format = "#,##0.00"

                # --- APPLY BORDERS & ALIGNMENT ---
                ws.row_dimensions[row].height = 140 # Fixed Height
                
                for col in range(1, 15):
                    cell = ws.cell(row=row, column=col)
                    cell.border = full_border 
                    
                    if col == 2:
                        cell.alignment = left_top_align
                    else:
                        cell.alignment = center_align

            except Exception as row_error:
                logger.error(f"Error processing row {row}: {row_error}")
                continue

        # --- B. TOTALS & FOOTER ---
        try:
            # Determine where the data ends
            last_data_row = START_ROW + max(actual_rows, 1) - 1
            
            TOTAL_ROW = last_data_row + 2
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1
              
            # ---- TOTAL AMOUNT ----
            ws.merge_cells(start_row=TOTAL_ROW, start_column=1, end_row=TOTAL_ROW, end_column=8)
            ws.cell(row=TOTAL_ROW, column=1, value="Total Amount (AED).")
            ws.cell(row=TOTAL_ROW, column=9, value=f"=SUM(I{START_ROW}:I{last_data_row})")
            
            # ---- VAT ----
            ws.merge_cells(start_row=VAT_ROW, start_column=1, end_row=VAT_ROW, end_column=8)
            ws.cell(row=VAT_ROW, column=1, value="VAT 5% (AED).")
            ws.cell(row=VAT_ROW, column=9, value=f"=I{TOTAL_ROW}*0.05")

            # ---- GRAND TOTAL ----
            ws.merge_cells(start_row=GRAND_ROW, start_column=1, end_row=GRAND_ROW, end_column=8)
            ws.cell(row=GRAND_ROW, column=1, value="GRAND TOTAL AMOUNT (AED).")
            ws.cell(row=GRAND_ROW, column=9, value=f"=SUM(I{TOTAL_ROW}:I{VAT_ROW})")

            # ---- APPLY FORMATTING FOR TOTAL/VAT ----
            for r in (TOTAL_ROW, VAT_ROW):
                ws.cell(row=r, column=1).font = red_bold
                ws.cell(row=r, column=9).font = red_bold

                ws.cell(row=r, column=1).alignment = right_align
                ws.cell(row=r, column=9).alignment = center_align
                ws.cell(row=r, column=9).number_format = "#,##0.00"

                # Borders for Total/VAT
                for c in range(1, 10):
                    ws.cell(row=r, column=c).border = thick_border

            # ---- GRAND TOTAL SPECIAL STYLE ----
            ws.cell(row=GRAND_ROW, column=1).font = red_bold_big
            ws.cell(row=GRAND_ROW, column=9).font = red_bold_big

            ws.cell(row=GRAND_ROW, column=1).alignment = right_align
            ws.cell(row=GRAND_ROW, column=9).alignment = center_align
            ws.cell(row=GRAND_ROW, column=9).number_format = "#,##0.00"

            # Borders & Fill for Grand Total
            for c in range(1, 10):
                cell = ws.cell(row=GRAND_ROW, column=c)
                cell.fill = yellow_fill
                cell.border = thick_border

            # Internal Profit Sum (Hidden/Side Column)


            # --- TERMS & CONDITIONS ---
            TERMS_ROW = GRAND_ROW + 2

            ws.merge_cells(start_row=TERMS_ROW, start_column=1, end_row=TERMS_ROW, end_column=9)
            t_cell = ws.cell(row=TERMS_ROW, column=1, value="TERMS:")
            t_cell.font = bold_font
            t_cell.fill = grey_fill
            t_cell.alignment = left_center_align

            # Border for "TERMS:" header
            for c in range(1, 10):
                ws.cell(row=TERMS_ROW, column=c).border = full_border

            # ---- TERMS ROWS LOOP ----
            terms_data = [
                ("Price:", ""),
                ("Delivery:", "Stated in Description Column Against Each Item."),
                ("Payment:", "30 Days Credit."),
                ("Validity:", "15 days from offer date.")
            ]

            for i, (label, value) in enumerate(terms_data):
                r = TERMS_ROW + 1 + i

                # Column A (Label)
                ws.cell(row=r, column=1, value=label)
                ws.cell(row=r, column=1).font = bold_font
                ws.cell(row=r, column=1).alignment = left_center_align

                # Columns B → I (Merged Value)
                ws.merge_cells(start_row=r, start_column=2, end_row=r, end_column=9)
                ws.cell(row=r, column=2, value=value)
                ws.cell(row=r, column=2).font = bold_font
                ws.cell(row=r, column=2).alignment = left_center_align

                # Borders for full row
                for c in range(1, 10):
                    ws.cell(row=r, column=c).border = full_border

            # --- FOOTER MESSAGES ---
            MSG_ROW = TERMS_ROW + len(terms_data) + 2
            ws.merge_cells(start_row=MSG_ROW, start_column=1, end_row=MSG_ROW, end_column=10)
            ws.cell(row=MSG_ROW, column=1, value="Please revert for clarifications if any.Thank you for providing an opportunity to quote.")
           
            
            ws.cell(row=MSG_ROW + 3, column=1, value="Best Regards,")

            disc_cell = ws.cell(row=MSG_ROW + 6, column=1, value="(This message has been electronically transmitted and does not require a signature).")
            disc_cell.font = italic_small

        except Exception as e:
            logger.error(f"Error writing totals/footer: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _to_float(self, value) -> float:
        try:
            if value is None: return 0.0
            s = str(value).strip().replace(",", "")
            return float(s) if s else 0.0
        except: 
            return 0.0