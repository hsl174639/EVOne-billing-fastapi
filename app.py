from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response
import pandas as pd
import io
import warnings

# 忽略 pandas 的一些格式警告
warnings.filterwarnings('ignore')

app = FastAPI(title="EV Billing Auto-Merge API")

# 测试接口：用于检查服务是否正常运行
@app.get("/")
def read_root():
    return {"status": "✅ 充电账单合并 API 正在运行！"}

# 核心处理接口：接收文件，返回 Excel
@app.post("/process-billing")
async def process_billing(
    gp_tx: UploadFile = File(..., description="GoParkin 交易 CSV"),
    gp_crm: UploadFile = File(..., description="GoParkin CRM Excel"),
    sp_tx: UploadFile = File(..., description="SP 交易 Excel"),
    sp_crm: UploadFile = File(..., description="SP CRM Excel")
):
    try:
        # ==========================================
        # 1. 将接收到的网络文件读取进内存
        # ==========================================
        gp_tx_bytes = await gp_tx.read()
        gp_crm_bytes = await gp_crm.read()
        sp_tx_bytes = await sp_tx.read()
        sp_crm_bytes = await sp_crm.read()

        # ==========================================
        # 2. GoParkin 数据处理逻辑
        # ==========================================
        # 处理 GoParkin CRM
        crm_gp = pd.read_excel(io.BytesIO(gp_crm_bytes))
        crm_gp = crm_gp[['Vehicle No.', 'Company']].dropna()
        crm_gp['Vehicle No.'] = crm_gp['Vehicle No.'].astype(str).str.strip().str.upper()

        # 处理 GoParkin 交易记录
        df_gp = pd.read_csv(io.BytesIO(gp_tx_bytes))
        df_gp = df_gp[df_gp['payment_status'] == 'Success'].copy()
        df_gp['vehicle_plate_number'] = df_gp['vehicle_plate_number'].astype(str).str.strip().str.upper()
        df_gp['Year-Month'] = df_gp['start_date_time'].astype(str).str[0:7]

        # 合并 GoParkin 数据并计算汇总
        gp_merged = pd.merge(df_gp, crm_gp, left_on='vehicle_plate_number', right_on='Vehicle No.', how='left')
        gp_merged['Company'] = gp_merged['Company'].fillna('Unmatched GoParkin')
        gp_summary = gp_merged.groupby(['Company', 'Year-Month'])['total_energy_supplied_kwh'].sum().reset_index()
        gp_summary.rename(columns={'total_energy_supplied_kwh': 'GoParkin(kWh)'}, inplace=True)

        # ==========================================
        # 3. SP 数据处理逻辑
        # ==========================================
        # 处理 SP CRM
        crm_sp = pd.read_excel(io.BytesIO(sp_crm_bytes))
        crm_sp = crm_sp[['Email', 'Company']].dropna()
        crm_sp['Email'] = crm_sp['Email'].astype(str).str.strip().str.lower()

        # 处理 SP 交易记录 (读取指定 sheet)
        df_sp = pd.read_excel(io.BytesIO(sp_tx_bytes), sheet_name='EVOne Corporate fleet')
        df_sp['Driver Email'] = df_sp['Driver Email'].astype(str).str.strip().str.lower()
        df_sp['Year-Month'] = df_sp['Date'].astype(str).str[0:7]
        df_sp['CDR Total Energy'] = pd.to_numeric(df_sp['CDR Total Energy'], errors='coerce').fillna(0)

        # 合并 SP 数据并计算汇总
        sp_merged = pd.merge(df_sp, crm_sp, left_on='Driver Email', right_on='Email', how='left')
        sp_merged['Company'] = sp_merged['Company'].fillna('Unmatched SP Email')
        sp_summary = sp_merged.groupby(['Company', 'Year-Month'])['CDR Total Energy'].sum().reset_index()
        sp_summary.rename(columns={'CDR Total Energy': 'SP(kWh)'}, inplace=True)

        # ==========================================
        # 4. 最终数据合并与规整
        # ==========================================
        final_df = pd.merge(gp_summary, sp_summary, on=['Company', 'Year-Month'], how='outer').fillna(0)
        final_df['Total(kWh)'] = final_df.get('GoParkin(kWh)', 0) + final_df.get('SP(kWh)', 0)

        # ==========================================
        # 5. 生成多 Sheet 的 Excel 文件（写回内存）
        # ==========================================
        output = io.BytesIO()
        months = sorted([m for m in final_df['Year-Month'].dropna().unique() if len(str(m)) == 7])
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for month in months:
                month_df = final_df[final_df['Year-Month'] == month].copy()
                month_df = month_df.sort_values(by='Company').reset_index(drop=True)
                month_df.insert(0, 'S/N', month_df.index + 1)  # 插入序号列
                month_df.to_excel(writer, sheet_name=month, index=False)

        # ==========================================
        # 6. 将生成的 Excel 作为 HTTP 附件返回
        # ==========================================
        excel_data = output.getvalue()
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=Monthly_Billing_Report.xlsx"}
        )

    except Exception as e:
        # 如果出错，返回错误信息以便在 n8n/Make 中排查
        return {"error": True, "message": str(e)}