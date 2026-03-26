from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response
from typing import List
import pandas as pd
import io
import warnings
import gc
import zipfile

# 引入生成 PDF 的专用库
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

warnings.filterwarnings('ignore')

app = FastAPI(title="EV Billing & PDF API")

@app.get("/")
def read_root():
    return {"status": "✅ API is running! PDF generator and Threshold Pricing enabled!"}

async def load_dataframe(file: UploadFile, sheet_name=None):
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
# 新接口：生成独立分月、分公司的 PDF 并根据 Threshold 算钱，最后打成 ZIP
# =====================================================================
@app.post("/process-pdf")
async def process_pdf(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm, rate_file = None, None, None, None, None
        
        # 1. 智能匹配文件（增加了识别 threshold and rate 的逻辑）
        for f in files:
            name = f.filename.lower()
            if 'threshold' in name or 'rate' in name: rate_file = f
            elif 'goparkin' in name and ('transaction' in name or 'row' in name): gp_tx = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name and ('report' in name or 'breakdown' in name or 'fleet' in name): sp_tx = f

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')
        
        # 2. 提取 Threshold 和 Rate 价格表，转化为字典方便查询
        rates_dict = {}
        if rate_file:
            df_rates = await load_dataframe(rate_file)
            for _, row in df_rates.iterrows():
                comp_name = str(row.get('company', '')).strip().lower()
                rates_dict[comp_name] = {
                    'base': pd.to_numeric(row.get('base', 0), errors='coerce'),
                    'threshold': pd.to_numeric(row.get('Threshold', 0), errors='coerce'),
                    'discounted': pd.to_numeric(row.get('discounted', 0), errors='coerce')
                }

        # ---------------- 数据清洗逻辑 (保留原有，确保准确) ----------------
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

        # 提取明细标准格式
        def extract_details(df, source):
            res = pd.DataFrame()
            if df.empty: return res
            res['Company'] = df['Company']
            if source == 'GP':
                res['Vehicle_Email'] = df['vehicle_plate_number']
                res['Start Time'] = df.get('start_date_time', df['end_date_time'])
                res['End Time'] = df['end_date_time']
                res['Location'] = df.get('site_name', df.get('station_name', 'GoParkin Station'))
                res['Energy (kWh)'] = df['total_energy_supplied_kwh']
            else:
                res['Vehicle_Email'] = df['Driver Email']
                res['Start Time'] = df.get('Start Date', df.get('Date', ''))
                res['End Time'] = df.get('End Date', df.get('Date', ''))
                res['Location'] = df.get('Location', df.get('Station Name', 'SP Station'))
                res['Energy (kWh)'] = df['CDR Total Energy']
            return res

        all_details = pd.concat([extract_details(gp_merged, 'GP'), extract_details(sp_merged, 'SP')], ignore_index=True)
        all_details = all_details[all_details['Energy (kWh)'] > 0]
        # 获取月份信息
        all_details['Year-Month'] = all_details['End Time'].astype(str).str[0:7]

        # ---------------- 生成 PDF 并装入内存 ZIP 文件夹 ----------------
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            
            months = all_details['Year-Month'].dropna().unique()
            for month in months:
                # 过滤出该月份的数据
                month_df = all_details[all_details['Year-Month'] == month]
                unique_companies = month_df['Company'].dropna().unique()
                
                for company in unique_companies:
                    comp_df = month_df[month_df['Company'] == company]
                    total_kwh = comp_df['Energy (kWh)'].sum()
                    
                    # 💡 核心定价逻辑：判断是否超额 Threshold
                    comp_key = str(company).strip().lower()
                    r_info = rates_dict.get(comp_key, {'base': 0, 'threshold': float('inf'), 'discounted': 0})
                    
                    base_rate = r_info['base'] if pd.notna(r_info['base']) else 0
                    threshold = r_info['threshold'] if pd.notna(r_info['threshold']) else float('inf')
                    discounted = r_info['discounted'] if pd.notna(r_info['discounted']) else 0
                    
                    # 没超过用 base，超过了用 discounted
                    applied_rate = discounted if total_kwh > threshold else base_rate
                    total_price = total_kwh * applied_rate

                    # ======= 开始绘制专业的 PDF =======
                    pdf_buf = io.BytesIO()
                    doc = SimpleDocTemplate(pdf_buf, pagesize=A4)
                    elements = []
                    styles = getSampleStyleSheet()
                    
                    # PDF 标题部分
                    elements.append(Paragraph(f"<b>Corporate Charging Statement</b>", styles['Title']))
                    elements.append(Spacer(1, 12))
                    elements.append(Paragraph(f"<b>Company:</b> {company}", styles['Normal']))
                    elements.append(Paragraph(f"<b>Billing Month:</b> {month}", styles['Normal']))
                    elements.append(Spacer(1, 20))
                    
                    # 1. 价格汇总表 (绿底白字表头)
                    elements.append(Paragraph("<b>1. Billing Summary</b>", styles['Heading2']))
                    summary_data = [
                        ["Total Energy (kWh)", "Threshold Limit", "Applied Rate ($)", "Total Amount ($)"],
                        [f"{total_kwh:.2f}", f"{threshold if threshold != float('inf') else 'N/A'}", f"${applied_rate:.2f}", f"${total_price:.2f}"]
                    ]
                    t_summary = Table(summary_data, colWidths=[120, 110, 110, 120])
                    t_summary.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1ABC9C')),
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0,0), (-1,0), 10),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.append(t_summary)
                    elements.append(Spacer(1, 24))
                    
                    # 2. 车辆明细表
                    elements.append(Paragraph("<b>2. Vehicle Breakdown</b>", styles['Heading2']))
                    veh_summary = comp_df.groupby('Vehicle_Email')['Energy (kWh)'].sum().reset_index().sort_values('Energy (kWh)', ascending=False)
                    veh_data = [["Vehicle / Driver Email", "Energy Used (kWh)"]]
                    for _, v_row in veh_summary.iterrows():
                        veh_data.append([str(v_row['Vehicle_Email']), f"{v_row['Energy (kWh)']:.2f}"])
                    
                    t_veh = Table(veh_data, colWidths=[250, 150])
                    t_veh.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#34495E')),
                        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    elements.append(t_veh)

                    # 生成此公司的 PDF
                    doc.build(elements)
                    
                    # 将 PDF 写入压缩包中，放在该月份对应的文件夹内
                    safe_comp = str(company)[:30].replace('/', '').replace(':', '').replace('*', '').replace('?', '')
                    # 路径格式：2026-02/Company_Name_2026-02.pdf
                    file_name = f"{month}/{safe_comp}_{month}.pdf"
                    zip_file.writestr(file_name, pdf_buf.getvalue())

        # 清理内存
        del df_gp, df_sp, gp_merged, sp_merged, all_details
        gc.collect()

        return Response(content=zip_buffer.getvalue(), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=Monthly_PDF_Reports.zip"})
    except Exception as e:
        return {"error": True, "message": str(e)}
