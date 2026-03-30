from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response
from typing import List
import pandas as pd
import io
import warnings
import gc
import zipfile
import os

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

warnings.filterwarnings('ignore')

app = FastAPI(title="EV Billing Ultimate API")

@app.get("/")
def read_root():
    return {"status": "✅ API is running! PDF now includes rate details in the header!"}

# --- 辅助函数：极致省内存的文件读取方式 ---
async def load_dataframe(file: UploadFile, sheet_name=None):
    if not file:
        raise ValueError("File is missing!")
    name = file.filename.lower()
    if name.endswith('.csv'):
        return pd.read_csv(file.file)
    elif name.endswith(('.xls', '.xlsx')):
        if sheet_name:
            try:
                return pd.read_excel(file.file, sheet_name=sheet_name)
            except Exception:
                file.file.seek(0)
                return pd.read_excel(file.file)
        return pd.read_excel(file.file)
    else:
        raise ValueError(f"Unsupported file format: {file.filename}")

# =====================================================================
# 接口 1：生成按月汇总表 Excel (带 5 列动态价格计算)
# =====================================================================
@app.post("/process-billing")
async def process_billing(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm, rate_file = None, None, None, None, None
        for f in files:
            name = f.filename.lower()
            if 'threshold' in name: rate_file = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif 'goparkin' in name: gp_tx = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name: sp_tx = f

        missing = []
        if not gp_tx: missing.append("GoParkin Transaction")
        if not gp_crm: missing.append("GoParkin CRM")
        if not sp_tx: missing.append("SP Transaction")
        if not sp_crm: missing.append("SP CRM")
        if not rate_file: missing.append("Threshold and Rate")
        
        if missing:
            received_names = [f.filename for f in files] if files else ["No files received"]
            return {
                "error": True, 
                "message": f"Missing files for: {', '.join(missing)}",
                "received_files_by_python": received_names
            }

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')
        df_rates = await load_dataframe(rate_file)
        
        rates_dict = {}
        for _, row in df_rates.iterrows():
            comp_name = str(row.get('company', '')).strip().lower()
            rates_dict[comp_name] = {
                'base': pd.to_numeric(row.get('base', 0), errors='coerce'),
                'threshold': pd.to_numeric(row.get('Threshold', 0), errors='coerce'),
                'discounted': pd.to_numeric(row.get('discounted', 0), errors='coerce')
            }

        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
        if 'transaction_type' in df_gp.columns:
            df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        df_gp['Year-Month'] = df_gp['end_date_time'].astype(str).str[0:7]
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')
        gp_summary = gp_merged.groupby(['Company', 'Year-Month'])['total_energy_supplied_kwh'].sum().reset_index()
        gp_summary.rename(columns={'total_energy_supplied_kwh': 'GoParkin(kWh)'}, inplace=True)

        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['Year-Month'] = df_sp['Date'].astype(str).str[0:7]
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')
        sp_summary = sp_merged.groupby(['Company', 'Year-Month'])['CDR Total Energy'].sum().reset_index()
        sp_summary.rename(columns={'CDR Total Energy': 'SP(kWh)'}, inplace=True)

        final_df = pd.merge(gp_summary, sp_summary, on=['Company', 'Year-Month'], how='outer').fillna(0)
        final_df['Total(kWh)'] = final_df.get('GoParkin(kWh)', 0) + final_df.get('SP(kWh)', 0)

        def calculate_pricing(row):
            comp_key = str(row['Company']).strip().lower()
            r_info = rates_dict.get(comp_key, {'base': 0, 'threshold': float('inf'), 'discounted': 0})
            
            base_rate = r_info['base'] if pd.notna(r_info['base']) else 0
            threshold = r_info['threshold'] if pd.notna(r_info['threshold']) else float('inf')
            discounted = r_info['discounted'] if pd.notna(r_info['discounted']) else 0
            
            applied_rate = discounted if row['Total(kWh)'] > threshold else base_rate
            total_price = row['Total(kWh)'] * applied_rate
            
            display_threshold = threshold if threshold != float('inf') else 'N/A'
            return pd.Series([display_threshold, base_rate, discounted, applied_rate, total_price])

        final_df[['Threshold', 'Base Rate ($)', 'Discounted Rate ($)', 'Applied Rate ($)', 'Total Price ($)']] = final_df.apply(calculate_pricing, axis=1)

        output = io.BytesIO()
        months = sorted([m for m in final_df['Year-Month'].dropna().unique() if len(str(m)) == 7])
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for month in months:
                month_df = final_df[final_df['Year-Month'] == month].copy()
                month_df = month_df.sort_values(by='Company').reset_index(drop=True)
                month_df.insert(0, 'S/N', month_df.index + 1)
                month_df.to_excel(writer, sheet_name=month, index=False)
                
                worksheet = writer.sheets[month]
                worksheet.set_column('B:B', 30)
                worksheet.set_column('D:L', 15)

        excel_data = output.getvalue()
        del df_gp, df_sp, gp_merged, sp_merged, final_df
        gc.collect()

        return Response(content=excel_data, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=Summary_Report.xlsx"})
    except Exception as e:
        return {"error": True, "message": str(e)}

# =====================================================================
# 接口 2：生成一公司一页的【绿色排版高级明细表 Excel】
# =====================================================================
@app.post("/process-details")
async def process_details(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm = None, None, None, None
        for f in files:
            name = f.filename.lower()
            if 'threshold' in name: pass
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif 'goparkin' in name: gp_tx = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name: sp_tx = f

        missing = []
        if not gp_tx: missing.append("GoParkin Transaction")
        if not gp_crm: missing.append("GoParkin CRM")
        if not sp_tx: missing.append("SP Transaction")
        if not sp_crm: missing.append("SP CRM")
        
        if missing:
            received_names = [f.filename for f in files] if files else ["No files received"]
            return {
                "error": True, 
                "message": f"Missing files for: {', '.join(missing)}",
                "received_files_by_python": received_names
            }

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')

        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
        if 'transaction_type' in df_gp.columns:
            df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')

        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')

        def extract_details(df, source):
            res = pd.DataFrame()
            if df.empty: return res
            res['Company'] = df['Company']
            if source == 'GP':
                res['Vehicle_Email'] = df['vehicle_plate_number']
                res['Start Time'] = df.get('start_date_time', df['end_date_time'])
                res['End Time'] = df['end_date_time']
                res['Location'] = df.get('carpark_code', df.get('site_name', 'GoParkin Station'))
                res['Energy (kWh)'] = df['total_energy_supplied_kwh']
            else:
                res['Vehicle_Email'] = df['Driver Email']
                res['Start Time'] = df.get('Start Date', df.get('Date', ''))
                res['End Time'] = df.get('End Date', df.get('Date', ''))
                res['Location'] = df.get('Location Name', df.get('Location', 'SP Station'))
                res['Energy (kWh)'] = df['CDR Total Energy']
            return res

        all_details = pd.concat([extract_details(gp_merged, 'GP'), extract_details(sp_merged, 'SP')], ignore_index=True)
        all_details = all_details[all_details['Energy (kWh)'] > 0]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'left'})
            header_green = workbook.add_format({'bg_color': '#00ad5f', 'font_color': 'white', 'bold': True, 'align': 'center', 'border': 1})
            footer_green = workbook.add_format({'bg_color': '#A3E4D7', 'bold': True, 'align': 'right', 'border': 1})
            cell_normal = workbook.add_format({'align': 'center', 'border': 1})

            unique_companies = all_details['Company'].dropna().unique()
            for company in unique_companies:
                safe_sheet_name = str(company)[:30].replace('/', '').replace(':', '').replace('*', '').replace('?', '')
                comp_df = all_details[all_details['Company'] == company]
                worksheet = workbook.add_worksheet(safe_sheet_name)
                worksheet.set_column(0, 0, 35)
                worksheet.set_column(1, 2, 22)
                worksheet.set_column(3, 3, 18)
                
                veh_summary = comp_df.groupby('Vehicle_Email')['Energy (kWh)'].sum().reset_index().sort_values('Energy (kWh)', ascending=False)
                worksheet.write(0, 0, f"[{company}] Total Vehicle Energy", title_fmt)
                worksheet.write(2, 0, "Vehicle / Driver Email", header_green)
                worksheet.write(2, 1, "Total Energy Used (kWh)", header_green)
                row = 3
                for _, v_row in veh_summary.iterrows():
                    worksheet.write(row, 0, v_row['Vehicle_Email'], cell_normal)
                    worksheet.write(row, 1, round(v_row['Energy (kWh)'], 3), cell_normal)
                    row += 1
                
                row += 3
                worksheet.write(row, 0, "Detailed Charging Log, title_fmt)
                row += 2
                for vehicle, grp in comp_df.groupby('Vehicle_Email'):
                    worksheet.merge_range(row, 0, row, 1, "Vehicle / Driver Email:", header_green)
                    worksheet.merge_range(row, 2, row, 3, str(vehicle), cell_normal)
                    row += 1
                    worksheet.write(row, 0, "Location", header_green)
                    worksheet.write(row, 1, "Start Time", header_green)
                    worksheet.write(row, 2, "End Time", header_green)
                    worksheet.write(row, 3, "Energy (kWh)", header_green)
                    row += 1
                    veh_total = 0
                    grp = grp.sort_values('Start Time')
                    for _, d_row in grp.iterrows():
                        worksheet.write(row, 0, str(d_row['Location']), cell_normal)
                        worksheet.write(row, 1, str(d_row['Start Time']), cell_normal)
                        worksheet.write(row, 2, str(d_row['End Time']), cell_normal)
                        worksheet.write(row, 3, round(d_row['Energy (kWh)'], 3), cell_normal)
                        veh_total += d_row['Energy (kWh)']
                        row += 1
                    worksheet.merge_range(row, 0, row, 2, "Total Power Usage:", footer_green)
                    worksheet.write(row, 3, round(veh_total, 3), footer_green)
                    row += 2

        excel_data = output.getvalue()
        del df_gp, df_sp, gp_merged, sp_merged, all_details
        gc.collect()

        return Response(content=excel_data, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=Detailed_Report.xlsx"})
    except Exception as e:
        return {"error": True, "message": str(e)}

# =====================================================================
# 接口 3：生成带有 Threshold 阶梯价格的【独立 PDF 压缩包】
# =====================================================================
@app.post("/process-pdf")
async def process_pdf(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm, rate_file = None, None, None, None, None
        
        for f in files:
            name = f.filename.lower()
            if 'threshold' in name: rate_file = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif 'goparkin' in name: gp_tx = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name: sp_tx = f

        missing = []
        if not gp_tx: missing.append("GoParkin Transaction")
        if not gp_crm: missing.append("GoParkin CRM")
        if not sp_tx: missing.append("SP Transaction")
        if not sp_crm: missing.append("SP CRM")
        if not rate_file: missing.append("Threshold and Rate")
        
        if missing:
            received_names = [f.filename for f in files] if files else ["No files received"]
            return {
                "error": True, 
                "message": f"Missing files for: {', '.join(missing)}",
                "received_files_by_python": received_names
            }

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')
        df_rates = await load_dataframe(rate_file)
        
        rates_dict = {}
        for _, row in df_rates.iterrows():
            comp_name = str(row.get('company', '')).strip().lower()
            rates_dict[comp_name] = {
                'base': pd.to_numeric(row.get('base', 0), errors='coerce'),
                'threshold': pd.to_numeric(row.get('Threshold', 0), errors='coerce'),
                'discounted': pd.to_numeric(row.get('discounted', 0), errors='coerce')
            }

        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
        if 'transaction_type' in df_gp.columns:
            df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')

        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')

        def extract_details(df, source):
            res = pd.DataFrame()
            if df.empty: return res
            res['Company'] = df['Company']
            if source == 'GP':
                res['Vehicle_Email'] = df['vehicle_plate_number']
                res['Start Time'] = df.get('start_date_time', df['end_date_time'])
                res['End Time'] = df['end_date_time']
                res['Location'] = df.get('carpark_code', df.get('site_name', 'GoParkin Station'))
                res['Energy (kWh)'] = df['total_energy_supplied_kwh']
            else:
                res['Vehicle_Email'] = df['Driver Email']
                res['Start Time'] = df.get('Start Date', df.get('Date', ''))
                res['End Time'] = df.get('End Date', df.get('Date', ''))
                res['Location'] = df.get('Location Name', df.get('Location', 'SP Station'))
                res['Energy (kWh)'] = df['CDR Total Energy']
            return res

        all_details = pd.concat([extract_details(gp_merged, 'GP'), extract_details(sp_merged, 'SP')], ignore_index=True)
        all_details = all_details[all_details['Energy (kWh)'] > 0]
        all_details['Year-Month'] = all_details['End Time'].astype(str).str[0:7]

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            months = all_details['Year-Month'].dropna().unique()
            for month in months:
                month_df = all_details[all_details['Year-Month'] == month]
                unique_companies = month_df['Company'].dropna().unique()
                
                for company in unique_companies:
                    comp_df = month_df[month_df['Company'] == company]
                    total_kwh = comp_df['Energy (kWh)'].sum()
                    
                    comp_key = str(company).strip().lower()
                    r_info = rates_dict.get(comp_key, {'base': 0, 'threshold': float('inf'), 'discounted': 0})
                    
                    base_rate = r_info['base'] if pd.notna(r_info['base']) else 0
                    threshold = r_info['threshold'] if pd.notna(r_info['threshold']) else float('inf')
                    discounted = r_info['discounted'] if pd.notna(r_info['discounted']) else 0
                    
                    applied_rate = discounted if total_kwh > threshold else base_rate
                    total_price = total_kwh * applied_rate

                    pdf_buf = io.BytesIO()
                    doc = SimpleDocTemplate(pdf_buf, pagesize=A4)
                    elements = []
                    styles = getSampleStyleSheet()
                    
                    # --- 0. 添加公司 Logo ---
                    logo_path = "logo.png"
                    if os.path.exists(logo_path):
                        logo_img = Image(logo_path, width=120, height=40) 
                        logo_img.hAlign = 'LEFT'
                        elements.append(logo_img)
                        elements.append(Spacer(1, 10))

                    # --- 1. PDF 标题部分 (新增计费参数明细) ---
                    elements.append(Paragraph(f"<b>Corporate Charging Statement</b>", styles['Title']))
                    elements.append(Spacer(1, 12))
                    elements.append(Paragraph(f"<b>Company:</b> {company}", styles['Normal']))
                    elements.append(Paragraph(f"<b>Billing Month:</b> {month}", styles['Normal']))
                    
                    # 动态格式化 Threshold 的显示
                    disp_thresh = f"{threshold:g}" if threshold != float('inf') else "N/A"
                    
                    elements.append(Paragraph(f"<b>Threshold Limit:</b> {disp_thresh}", styles['Normal']))
                    elements.append(Paragraph(f"<b>Base Rate:</b> ${base_rate:.2f}", styles['Normal']))
                    elements.append(Paragraph(f"<b>Discounted Rate:</b> ${discounted:.2f}", styles['Normal']))
                    elements.append(Paragraph(f"<b>Applied Rate:</b> ${applied_rate:.2f}", styles['Normal']))
                    
                    elements.append(Spacer(1, 20))
                    
                    # --- 2. 价格汇总表 ---
                    elements.append(Paragraph("<b>1. Billing Summary</b>", styles['Heading2']))
                    summary_data = [
                        ["Total Energy (kWh)", "Threshold Limit", "Applied Rate ($)", "Total Amount ($)"],
                        [f"{total_kwh:.2f}", f"{threshold if threshold != float('inf') else 'N/A'}", f"${applied_rate:.2f}", f"${total_price:.2f}"]
                    ]
                    t_summary = Table(summary_data, colWidths=[120, 110, 110, 120])
                    t_summary.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke), 
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0,0), (-1,0), 10),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.append(t_summary)
                    elements.append(Spacer(1, 24))
                    
                    # --- 3. 车辆用量汇总表 ---
                    elements.append(Paragraph("<b>2. Vehicle Breakdown</b>", styles['Heading2']))
                    veh_summary = comp_df.groupby('Vehicle_Email')['Energy (kWh)'].sum().reset_index().sort_values('Energy (kWh)', ascending=False)
                    veh_data = [["Vehicle / Driver Email", "Energy Used (kWh)"]]
                    for _, v_row in veh_summary.iterrows():
                        veh_data.append([str(v_row['Vehicle_Email']), f"{v_row['Energy (kWh)']:.2f}"])
                    
                    t_veh = Table(veh_data, colWidths=[250, 150])
                    t_veh.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke), 
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.append(t_veh)
                    elements.append(Spacer(1, 24))

                    # --- 4. 详细充电子表 ---
                    elements.append(Paragraph("<b>3. Detailed Charging Log</b>", styles['Heading2']))
                    elements.append(Spacer(1, 10))
                    
                    for vehicle, grp in comp_df.groupby('Vehicle_Email'):
                        elements.append(Paragraph(f"<b>Vehicle / Driver Email:</b> {vehicle}", styles['Normal']))
                        elements.append(Spacer(1, 6))
                        
                        detail_data = [["Location", "Start Time", "End Time", "Energy (kWh)"]]
                        grp = grp.sort_values('Start Time')
                        veh_total = 0
                        for _, d_row in grp.iterrows():
                            detail_data.append([
                                str(d_row['Location']), 
                                str(d_row['Start Time']), 
                                str(d_row['End Time']), 
                                f"{d_row['Energy (kWh)']:.2f}"
                            ])
                            veh_total += d_row['Energy (kWh)']
                        
                        detail_data.append(["", "", "Total:", f"{veh_total:.2f}"])
                        
                        t_detail = Table(detail_data, colWidths=[170, 100, 100, 80])
                        t_detail.setStyle(TableStyle([
                            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00ad5f')), 
                            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                            ('FONTNAME', (2,-1), (2,-1), 'Helvetica-Bold'), 
                            ('FONTNAME', (3,-1), (3,-1), 'Helvetica-Bold'), 
                            ('BACKGROUND', (0,-1), (-1,-1), colors.whitesmoke), 
                        ]))
                        elements.append(t_detail)
                        elements.append(Spacer(1, 16))

                    doc.build(elements)
                    
                    safe_comp = str(company)[:30].replace('/', '').replace(':', '').replace('*', '').replace('?', '')
                    file_name = f"{month}/{safe_comp}_{month}.pdf"
                    zip_file.writestr(file_name, pdf_buf.getvalue())

        del df_gp, df_sp, gp_merged, sp_merged, all_details, df_rates
        gc.collect()

        return Response(content=zip_buffer.getvalue(), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=Monthly_PDF_Reports.zip"})
    except Exception as e:
        return {"error": True, "message": str(e)}
