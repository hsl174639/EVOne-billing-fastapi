from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response
from typing import List
import pandas as pd
import io
import warnings
import gc

warnings.filterwarnings('ignore')

app = FastAPI(title="EV Billing & Details API")

@app.get("/")
def read_root():
    return {"status": "✅ API 正在运行，已将 Corporate 筛选应用于 GoParkin，包含双接口！"}

# --- 辅助函数：极致省内存的文件读取方式 ---
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
        raise ValueError(f"不支持的文件格式: {file.filename}")

# =====================================================================
# 接口 1：生成无明细的【按月汇总表】 
# =====================================================================
@app.post("/process-billing")
async def process_billing(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm = None, None, None, None
        for f in files:
            name = f.filename.lower()
            if 'goparkin' in name and ('transaction' in name or 'row' in name): gp_tx = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name and ('report' in name or 'breakdown' in name or 'fleet' in name): sp_tx = f

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')

        # ---------------- GoParkin 处理 ----------------
        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        
        # 1. 过滤支付状态
        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
            
        # 2. 【关键修改】过滤交易类型为 Corporate (应用于 GoParkin)
        if 'transaction_type' in df_gp.columns:
            df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
            
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        df_gp['Year-Month'] = df_gp['end_date_time'].astype(str).str[0:7]
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')
        gp_summary = gp_merged.groupby(['Company', 'Year-Month'])['total_energy_supplied_kwh'].sum().reset_index()
        gp_summary.rename(columns={'total_energy_supplied_kwh': 'GoParkin(kWh)'}, inplace=True)

        # ---------------- SP 处理 ----------------
        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        
        # SP 不再进行 Corporate 筛选，直接读取并清洗格式
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['Year-Month'] = df_sp['Date'].astype(str).str[0:7]
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')
        sp_summary = sp_merged.groupby(['Company', 'Year-Month'])['CDR Total Energy'].sum().reset_index()
        sp_summary.rename(columns={'CDR Total Energy': 'SP(kWh)'}, inplace=True)

        # ---------------- 合并生成汇总 ----------------
        final_df = pd.merge(gp_summary, sp_summary, on=['Company', 'Year-Month'], how='outer').fillna(0)
        final_df['Total(kWh)'] = final_df.get('GoParkin(kWh)', 0) + final_df.get('SP(kWh)', 0)

        output = io.BytesIO()
        months = sorted([m for m in final_df['Year-Month'].dropna().unique() if len(str(m)) == 7])
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for month in months:
                month_df = final_df[final_df['Year-Month'] == month].copy()
                month_df = month_df.sort_values(by='Company').reset_index(drop=True)
                month_df.insert(0, 'S/N', month_df.index + 1)
                month_df.to_excel(writer, sheet_name=month, index=False)

        excel_data = output.getvalue()
        del df_gp, df_sp, gp_merged, sp_merged, final_df
        gc.collect()

        return Response(content=excel_data, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=Summary_Report.xlsx"})
    except Exception as e:
        return {"error": True, "message": str(e)}

# =====================================================================
# 接口 2：生成一公司一页的【绿色排版高级明细表】
# =====================================================================
@app.post("/process-details")
async def process_details(files: List[UploadFile] = File(...)):
    try:
        gp_tx, gp_crm, sp_tx, sp_crm = None, None, None, None
        for f in files:
            name = f.filename.lower()
            if 'goparkin' in name and ('transaction' in name or 'row' in name): gp_tx = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name): gp_crm = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name): sp_crm = f
            elif 'evone' in name and ('report' in name or 'breakdown' in name or 'fleet' in name): sp_tx = f

        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')

        # ---------------- GoParkin 清洗 ----------------
        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')
        
        # 1. 过滤支付状态
        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
            
        # 2. 【关键修改】过滤交易类型为 Corporate (应用于 GoParkin)
        if 'transaction_type' in df_gp.columns:
            df_gp = df_gp[df_gp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()
            
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')

        # ---------------- SP 清洗 ----------------
        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')
        
        # SP 不做 Corporate 筛选
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')

        # ---------------- 提取明细标准格式 ----------------
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

        # ---------------- 生成高级排版 Excel ----------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'left'})
            header_green = workbook.add_format({'bg_color': '#1ABC9C', 'font_color': 'white', 'bold': True, 'align': 'center', 'border': 1})
            footer_green = workbook.add_format({'bg_color': '#A3E4D7', 'bold': True, 'align': 'right', 'border': 1})
            cell_normal = workbook.add_format({'align': 'center', 'border': 1})

            unique_companies = all_details['Company'].dropna().unique()
            for company in unique_companies:
                safe_sheet_name = str(company)[:30].replace('/', '').replace(':', '').replace('*', '').replace('?', '')
                comp_df = all_details[all_details['Company'] == company]
                worksheet = workbook.add_worksheet(safe_sheet_name)
                worksheet.set_column(0, 0, 25)
                worksheet.set_column(1, 2, 20)
                worksheet.set_column(3, 3, 15)
                
                veh_summary = comp_df.groupby('Vehicle_Email')['Energy (kWh)'].sum().reset_index().sort_values('Energy (kWh)', ascending=False)
                worksheet.write(0, 0, f"【{company}】 车辆电量总计", title_fmt)
                worksheet.write(2, 0, "车辆 / 邮箱", header_green)
                worksheet.write(2, 1, "总使用电量 (kWh)", header_green)
                row = 3
                for _, v_row in veh_summary.iterrows():
                    worksheet.write(row, 0, v_row['Vehicle_Email'], cell_normal)
                    worksheet.write(row, 1, round(v_row['Energy (kWh)'], 3), cell_normal)
                    row += 1
                
                row += 3
                worksheet.write(row, 0, "==== 详细充电流水 ====", title_fmt)
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
