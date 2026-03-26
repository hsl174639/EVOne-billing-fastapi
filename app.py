from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response
from typing import List
import pandas as pd
import io
import warnings
import gc

warnings.filterwarnings('ignore')

app = FastAPI(title="EV Billing Auto-Merge API")

@app.get("/")
def read_root():
    return {"status": "✅ API 正在运行，已开启 SP Corporate 交易筛选！"}

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

@app.post("/process-billing")
async def process_billing(files: List[UploadFile] = File(..., description="一次性上传所有源文件")):
    try:
        # ==========================================
        # 1. 文件名智能模糊匹配 (Fuzzy Matching)
        # ==========================================
        gp_tx, gp_crm, sp_tx, sp_crm = None, None, None, None
        
        for f in files:
            name = f.filename.lower()
            
            if 'goparkin' in name and ('transaction' in name or 'row' in name):
                gp_tx = f
            elif 'goparkin' in name and ('crm' in name or 'vehicle' in name):
                gp_crm = f
            elif ('sp ' in name or '_sp' in name or 'sp_' in name) and ('crm' in name or 'vehicle' in name):
                sp_crm = f
            elif 'evone' in name and ('report' in name or 'breakdown' in name or 'fleet' in name):
                sp_tx = f

        missing = []
        if not gp_tx: missing.append("GoParkin 交易记录")
        if not gp_crm: missing.append("GoParkin CRM")
        if not sp_tx: missing.append("SP 交易记录")
        if not sp_crm: missing.append("SP CRM")
        
        if missing:
            return {"error": True, "message": f"文件匹配失败: {', '.join(missing)}"}

        # ==========================================
        # 2. 动态读取数据 (支持 CSV / Excel)
        # ==========================================
        crm_gp = await load_dataframe(gp_crm)
        df_gp  = await load_dataframe(gp_tx)
        crm_sp = await load_dataframe(sp_crm)
        df_sp  = await load_dataframe(sp_tx, sheet_name='EVOne Corporate fleet')

        # ==========================================
        # 3. GoParkin 数据处理逻辑
        # ==========================================
        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()
        # 强制 CRM 去重，防止数据合并时裂变翻倍
        crm_gp = crm_gp.drop_duplicates(subset=['Vehicle No.'], keep='first')

        if 'payment_status' in df_gp.columns:
            df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
            
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        df_gp['Year-Month'] = df_gp['end_date_time'].astype(str).str[0:7]

        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')
        gp_summary = gp_merged.groupby(['Company', 'Year-Month'])['total_energy_supplied_kwh'].sum().reset_index()
        gp_summary.rename(columns={'total_energy_supplied_kwh': 'GoParkin(kWh)'}, inplace=True)

        # ==========================================
        # 4. SP 数据处理逻辑
        # ==========================================
        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()
        # 强制 CRM 去重
        crm_sp = crm_sp.drop_duplicates(subset=['Email'], keep='first')

        # 👇 【关键新增】：只提取交易类型为 'Corporate' 的记录
        if 'transaction_type' in df_sp.columns:
            # 加上了转小写匹配，防止原表格里出现 'corporate', 'Corporate ', 'CORPORATE' 等格式不统一导致漏算
            df_sp = df_sp[df_sp['transaction_type'].astype(str).str.strip().str.lower() == 'corporate'].copy()

        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['Year-Month'] = df_sp['Date'].astype(str).str[0:7]
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)

        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')
        sp_summary = sp_merged.groupby(['Company', 'Year-Month'])['CDR Total Energy'].sum().reset_index()
        sp_summary.rename(columns={'CDR Total Energy': 'SP(kWh)'}, inplace=True)

        # ==========================================
        # 5. 最终数据合并与规整
        # ==========================================
        final_df = pd.merge(gp_summary, sp_summary, on=['Company', 'Year-Month'], how='outer').fillna(0)
        final_df['Total(kWh)'] = final_df.get('GoParkin(kWh)', 0) + final_df.get('SP(kWh)', 0)

        # ==========================================
        # 6. 生成无明细的多 Sheet Excel 报表
        # ==========================================
        output = io.BytesIO()
        months = sorted([m for m in final_df['Year-Month'].dropna().unique() if len(str(m)) == 7])
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # 只写入按月份分类的汇总表，去掉了明细表
            for month in months:
                month_df = final_df[final_df['Year-Month'] == month].copy()
                month_df = month_df.sort_values(by='Company').reset_index(drop=True)
                month_df.insert(0, 'S/N', month_df.index + 1)
                month_df.to_excel(writer, sheet_name=month, index=False)

        excel_data = output.getvalue()

        # 清理内存，防止连续运行后 OOM
        del df_gp, df_sp, gp_merged, sp_merged, final_df
        gc.collect()

        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=Monthly_Billing_Report.xlsx"}
        )

    except Exception as e:
        return {"error": True, "message": str(e)}
