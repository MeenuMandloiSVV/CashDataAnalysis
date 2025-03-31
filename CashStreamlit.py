import streamlit as st
import pandas as pd
import os
import plotly.express as px
from pymongoarrow.api import find_pandas_all
import streamlit.components.v1 as components
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
# from MongoDBconnection import MongoDBProcessor
from pymongo import MongoClient

# UPLOAD_DIR = "uploads"  # Directory to store uploaded files
# os.makedirs(UPLOAD_DIR, exist_ok=True)  # Ensure the directory exists

class CSVReaderApp():
    def __init__(self):
        # self.folder_path = r"D:\CashIndexSwings\StreamlitFiles"
        self.IndexSwings = ""
        self.month_high_data = None
        self.client = MongoClient("mongodb+srv://meenu:8770302706@cluster0.0jvqc5o.mongodb.net/")
        self.db = self.client["cashdataanalysis"]
        self.files = {"Index Swing": None, "Close to Close": None, "Month on Month": None}
        self.dataframes = {}
    
    # def load(self):
    #     a = list(self.db['indexswings'].find({},{"_id": 0}))
    #     b = list(self.db['closetoclose'].find({},{"_id": 0}))
    #     c = list(self.db['monthonmonth'].find({},{"_id": 0}))
    #     self.dfa = pd.DataFrame(a)
    #     st.write(self.dfa.head())
    #     self.dfb = pd.DataFrame(b)
    #     st.write(self.dfb.head())
    #     self.dfc = pd.DataFrame(c)
    #     st.write(self.dfc.head())

    def load(self):
        self.dfa = find_pandas_all(self.db['indexswings'], {}, {"_id": 0})
        self.dfb = find_pandas_all(self.db['closetoclose'], {}, {"_id": 0})
        self.dfc = find_pandas_all(self.db['monthonmonth'], {}, {"_id": 0})

        print(self.dfa.head())
        print(self.dfb.head())
        print(self.dfc.head())
        
    # def upload(self):
    #     with st.sidebar.expander("📂 File Actions", expanded=False):  # Wrap everything inside expander
    #         st.write("## Upload Files")
    #         for key in self.files.keys():
    #             if self.files[key] is None:  # Only show uploader if no file exists
    #                 uploaded_file = st.file_uploader(f"Upload {key}", key=key)
    #                 if uploaded_file is not None:
    #                     file_path = os.path.join(UPLOAD_DIR, uploaded_file.name)
    #                     with open(file_path, "wb") as f:
    #                         f.write(uploaded_file.getbuffer())  # Save file to disk
    #                     self.files[key] = file_path  # Store file path
    #                     self.dataframes[key] = pd.read_csv(file_path)  # Read file into DataFrame

    #         # Button to show uploaded files
    #         if st.button("Show All Uploaded Files"):
    #             self.show_uploaded_files()

    # def show_uploaded_files(self):
    #     st.write("## Uploaded Files Preview")

    #     if not self.dataframes:
    #         st.write("No files uploaded yet.")
    #         return

    #     for key, file_path in list(self.files.items()):
    #         if file_path is not None and key in self.dataframes:
    #             df = self.dataframes[key]
    #             col1, col2 = st.columns([4, 1])  # Layout for buttons
                
    #             with col1:
    #                 st.write(f"### {key} - {os.path.basename(file_path)}")  # Show file name
    #                 st.write(df.head())  # Show first 5 rows

    #             with col2:
    #                 # Delete Button
    #                 if st.button(f"❌ Delete {key}", key=f"delete_{key}"):
    #                     os.remove(file_path)  # Delete file from disk
    #                     self.files[key] = None
    #                     del self.dataframes[key]
    #                     st.experimental_rerun()  # Refresh UI

    #                 # Replace Button
    #                 new_file = st.file_uploader(f"Replace {key}", key=f"replace_{key}")
    #                 if new_file is not None:
    #                     new_file_path = os.path.join(UPLOAD_DIR, new_file.name)
    #                     with open(new_file_path, "wb") as f:
    #                         f.write(new_file.getbuffer())  # Save new file
    #                     self.files[key] = new_file_path
    #                     self.dataframes[key] = pd.read_csv(new_file_path)
    #                     st.experimental_rerun()  # Refresh UI
                        
                        
    def rank_month_end_dates(self, df):
        # Convert dates to datetime format (if not already)
        df['IndexStartDate'] = pd.to_datetime(df['IndexStartDate'])
        df['MonthEndDate'] = pd.to_datetime(df['MonthEndDate'])

        # Extract year-month from IndexStartDate and MonthEndDate
        df['IndexMonth'] = df['IndexStartDate'].dt.to_period('M')
        df['EndMonth'] = df['MonthEndDate'].dt.to_period('M')

        # Rank MonthEndDate within each IndexStartDate group
        df['MonthRank'] = df.groupby('IndexStartDate')['EndMonth'].rank(method='dense').astype(int) - 1

        # Drop helper columns
        df.drop(columns=['IndexMonth', 'EndMonth'], inplace=True)

        return df
    

    def styled_dataframe(self, df, height=500, width='100%'):
        if df.empty:
            st.warning("⚠️ No data available to display.")
            return

        formatted_df = df.copy()

        # **Initialize nifty_rows to avoid errors**
        nifty_rows = pd.DataFrame()

        if 'Symbol' in formatted_df.columns:
            # **Separate NIFTY rows**
            nifty_rows = formatted_df[formatted_df["Symbol"] == "NIFTY"]
            formatted_df = formatted_df[formatted_df["Symbol"] != "NIFTY"]  # 🔥 **Remove NIFTY rows to avoid duplication**

        numeric_cols = formatted_df.select_dtypes(include=['number']).columns
        formatted_df[numeric_cols] = formatted_df[numeric_cols].replace([10**13, -10**13], pd.NA)
        
        # Apply formatting in one step
        def format_value(x):
            if pd.isna(x):
                return x
            elif x % 1 == 0:
                return int(x)
            else:
                return round(x, 2)
        
        formatted_df[numeric_cols] = formatted_df[numeric_cols].applymap(format_value)

        # **Grid Options**
        gb = GridOptionsBuilder.from_dataframe(formatted_df)
        gb.configure_default_column(filter=True, sortable=True, resizable=True, wrapHeaderText=True, autoHeaderHeight=True)

        # **Pin NIFTY rows only if available**
        if not nifty_rows.empty:
            gb.configure_grid_options(pinnedTopRowData=nifty_rows.to_dict('records'))

        if len(formatted_df.columns) >= 2:
            gb.configure_column(formatted_df.columns[0], pinned='left')
            gb.configure_column(formatted_df.columns[1], pinned='left')
            
            
        font_size = "18px"
        cell_style_jscode = JsCode(f"""
        function(params) {{
            var baseStyle = {{
                'fontSize': '{font_size}',
                'padding': '6px'
            }};
            if (params.value > 0) {{
                return {{ ...baseStyle, 'backgroundColor': '#d4edda' }} // Green
            }} else if (params.value < 0) {{
                return {{ ...baseStyle, 'backgroundColor': '#f8d7da' }} // Red
            }} else if (params.value == 0) {{
                return {{ ...baseStyle, 'backgroundColor': '#fff3cd' }} // Yellow
            }} else {{
                return baseStyle;
            }}
        }}
        """)

        for col in numeric_cols:
            gb.configure_column(col, cellStyle=cell_style_jscode)

        # **🔥 Cell Click Listener (New Feature)**
        cell_click_jscode = JsCode("""
        function(event) {
            if (event.column && event.value) {
                return { "column": event.column.colId, "value": event.value };
            } else {
                return null;
            }
        }
        """)
        gb.configure_grid_options(onCellClicked=cell_click_jscode)

        grid_options = gb.build()

       

        # **🔥 Display Grid**
        grid_response = AgGrid(
            formatted_df,
            gridOptions=grid_options,
            height=height,
            width=width,
            fit_columns_on_grid_load=False,
            theme="alpine",
            allow_unsafe_jscode=True, 
            custom_css={
                ".ag-header-cell-label": {
                    "white-space": "normal !important",
                    "line-height": "1.4",
                    "font-size": "20px",
                    "text-align": "center"
                },
                ".ag-cell": {
                    "font-size": '18px',
                    "line-height": "35px",
                    "text-align": "center",
                    "justify-content": "center",
                    "display": "flex",
                    "align-items": "center"
                }
            }
        )

        # **🔥 Show Clicked Cell Data in Sidebar**
        if grid_response and "selectedRows" in grid_response:
            clicked_data = grid_response["selectedRows"]
            if clicked_data:
                st.sidebar.write("### 🔹 Selected Cell Details")
                st.sidebar.json(clicked_data[0])

        # **🔥 Permanent Horizontal Scroll**
        st.markdown("""
        <style>
            .ag-root-wrapper {
                overflow-x: auto !important;
            }
        </style>
        """, unsafe_allow_html=True)






    def IndexDatePivot(self):
        df = self.month_high_data.copy()
        
        
        # Apply month ranking and filter based on user input
        df = self.rank_month_end_dates(df)
        user_input = st.number_input("Enter Month Rank (e.g., 12)", min_value=0, max_value=100, value=12, step=1)
        df = df[df['MonthRank'] == user_input]  # Ensure only selected rank is included
        
        # Ensure IndexStartDate is string (fix for TypeError)
        df['IndexStartDate'] = df['IndexStartDate'].astype(str)
        
        # Define range buckets
        bins = [-100, -90, -80, -70, -60, -50, -40, -30, -20, -10, 0, 
                20, 40, 70, 100, 300, 1000, 10000000]
        labels = [f"{low} to {high}" for low, high in zip(bins[:-1], bins[1:])]

        # Fetch stock change data from `self.CaseClosetoClosedata`
        stock_data = self.CaseClosetoClosedata[['Symbol', 'StockChange', 'IdxStart_Date']].copy()
        
        # Convert `IdxStart_Date` to string to match `IndexStartDate`
        stock_data['IdxStart_Date'] = stock_data['IdxStart_Date'].astype(str)

        # Categorize `StockChange` values into defined ranges
        stock_data['Range'] = pd.cut(stock_data['StockChange'], bins=bins, labels=labels, include_lowest=True)

        # Merge range data back to main DataFrame
        df = df.merge(stock_data, left_on=['Symbol', 'IndexStartDate'], right_on=['Symbol', 'IdxStart_Date'], how="left")
        df.to_csv('df.csv')
        # Pivot table to calculate mean percent change for each IndexStartDate & Range
        pivot_df = df.groupby(['Range', 'IndexStartDate'], dropna=False)['StkPercChange'].mean().unstack()

        # Convert range list to DataFrame
        range_df = pd.DataFrame({"Range": labels})

        # Use outer join to ensure all ranges are retained
        pivot_df = range_df.merge(pivot_df, on="Range", how="outer").sort_values(by="Range")

        # Convert column names to string (for st_aggrid compatibility)
        pivot_df.columns = pivot_df.columns.astype(str)

        # Display DataFrame
        self.styled_dataframe(pivot_df)

            
    def select_csv_file(self):
        st.sidebar.header("CSV File Selector")
        if self.folder_path and os.path.isdir(self.folder_path):
            csv_files = [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]
            if csv_files:

                # Logic for selecting defaults based on filename
                index_csv = next((f for f in csv_files if "swingpoints" in f.lower()), csv_files[0])
                close_csv = next((f for f in csv_files if "close" in f.lower() or "case1" in f.lower()), csv_files[0])
                month_high_csv = next((f for f in csv_files if "month" in f.lower() or "case2" in f.lower()), csv_files[0])

                self.IndexSwings = st.sidebar.selectbox(
                    "Select Index Swings CSV file:",
                    csv_files,
                    index=csv_files.index(index_csv)
                )
                self.ClosetoCloseStocksFile = st.sidebar.selectbox(
                    "Select Close to Close Case 1 CSV file (for date range):",
                    csv_files,
                    index=csv_files.index(close_csv)
                )
                self.MonthHighFile = st.sidebar.selectbox(
                    "Select Monthly High CSV file (Case 2):",
                    csv_files,
                    index=csv_files.index(month_high_csv)
                )
            else:
                st.sidebar.warning("No CSV files found in this folder.")
        elif self.folder_path:
            st.sidebar.error("Invalid folder path!")


    def read_csv(self):
        if self.IndexSwings:
            file_path = os.path.join(self.folder_path, self.IndexSwings)
            try:
                self.indexswingsdata = pd.read_csv(file_path)
            except Exception as e:
                st.error(f"Failed to read Index Swings CSV: {e}")

        if self.ClosetoCloseStocksFile:
            additional_file_path = os.path.join(self.folder_path, self.ClosetoCloseStocksFile)
            try:
                self.CaseClosetoClosedata = pd.read_csv(additional_file_path, parse_dates=['IdxStart_Date', 'IdxEnd_Date'])
            except Exception as e:
                st.error(f"Failed to read Close to Close CSV: {e}")

        if self.MonthHighFile:
            month_high_file_path = os.path.join(self.folder_path, self.MonthHighFile)
            try:
                self.month_high_data = pd.read_csv(month_high_file_path)
            except Exception as e:
                st.error(f"Failed to read Monthly High CSV: {e}")


    def McapSectorIndustryData(self):
        if self.month_high_data is not None:
            # st.markdown(f"### 🔍 Preview of **{self.IndexSwings}**")

            # Create columns for the filters in a row
            col1, col2, col3 = st.columns(3)

            with col1:
                option = st.selectbox("Select Type", ["Mcap", "Sector", "Industry"])

            with col2:
                index_dates = self.month_high_data['IndexDate'].unique()
                selected_index_date = st.selectbox("Select IndexDate", sorted(index_dates))

            with col3:
                filtered_index = self.month_high_data[self.month_high_data['IndexDate'] == selected_index_date]
                month_end_dates = filtered_index['MonthEndDate'].unique()
                selected_month_end_date = st.selectbox("Select MonthEndDate", sorted(month_end_dates))

            mapping = {
                "Mcap": {"y_col": "McapAvgPercChange", "x_col": "mcaptype"},
                "Sector": {"y_col": "SectorAvgPercChange", "x_col": "sectorname"},
                "Industry": {"y_col": "IndustryAvgPercChange", "x_col": "industryname"}
            }

            selected_cols = mapping[option]
            y_col = selected_cols["y_col"]
            x_col = selected_cols["x_col"]

            final_df = filtered_index[filtered_index['MonthEndDate'] == selected_month_end_date]
            final_df = final_df.drop_duplicates(subset=[x_col, 'MonthEndDate', 'IndexDate'])
            
            # 👇 Dynamic Multi-Select Filter based on selection
            multi_select_values = final_df[x_col].unique()
            selected_values = st.multiselect(f"Select {option} items to filter:", multi_select_values)

            # Apply filter only if user selected any items
            if selected_values:
                final_df = final_df[final_df[x_col].isin(selected_values)]

            # Sort & Color setup
            final_df = final_df.sort_values(by=y_col, ascending=False)
            final_df['color'] = final_df[y_col].apply(lambda x: 'green' if x >= 0 else 'red')

            symbol_count = filtered_index[filtered_index['MonthEndDate'] == selected_month_end_date].groupby(x_col)['Symbol'].count().reset_index()
            symbol_count.rename(columns={'Symbol': 'No_of_Stocks'}, inplace=True)
            final_df = final_df.merge(symbol_count, on=x_col, how='left')
            final_df[y_col] = final_df[y_col].round(2)


        # Rest of your plotting + AgGrid logic stays the same...

            
            # Plot Graph
            if not final_df.empty:
                fig = px.bar(
                    final_df,
                    x=x_col,
                    y=y_col,
                    title=f"{option} % Change on {selected_month_end_date} ({selected_index_date})",
                    labels={y_col: f"{option} % Change"},
                )

                fig.update_traces(
                    marker_color=final_df['color'],
                    width=0.8
                )
                fig.update_layout(
                    xaxis=dict(
                        categoryorder='total descending',
                        tickangle=-45,
                        tickmode='linear'
                    ),
                    bargap=0.3,
                    bargroupgap=0.2,
                    margin=dict(l=40, r=40, t=60, b=150),
                    xaxis_title=x_col,
                    yaxis_title=f"{option} % Change",
                    height=600,
                    width=3000
                )

                # Export graph as HTML
                graph_html = fig.to_html(full_html=False)

                st.markdown("<h3 style='text-align: center;'> 📈 Avg % Change Graph</h3>", unsafe_allow_html=True)
                components.html(
                    f"""
                    <div style="overflow-x: auto; border: 1px solid #ddd; padding: 10px;">
                        {graph_html}
                    </div>
                    """,
                    height=650
                )
                

            # Table Section
            st.markdown("<h3 style='text-align: center;'>📋 Avg % Table</h3>", unsafe_allow_html=True)

            
        
            gb = GridOptionsBuilder.from_dataframe(final_df[[x_col, y_col, 'No_of_Stocks', 'IndexDate', 'MonthEndDate']])
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=10)

            gb.configure_default_column(
                groupable=True,
                value=True,
                enableRowGroup=True,
                aggFunc='sum',
                editable=False,
                resizable=True,
                minWidth=150,
                flex=1
            )

            # Individual adjustments
            gb.configure_column(x_col, minWidth=220, flex=2)
            gb.configure_column(y_col, minWidth=220, flex=2)


            gb.configure_side_bar()
            gb.configure_selection(selection_mode="single", use_checkbox=False)
            gb.configure_grid_options(rowHeight=50)

            grid_options = gb.build()
            grid_options['pagination'] = True
            grid_options['paginationPageSize'] = 10
            grid_options['domLayout'] = 'autoHeight'  # Change this to autoHeight for scroll
            grid_options["headerHeight"] = 60

            custom_css = {
                ".ag-theme-balham .ag-cell": {
                    "font-size": "25px",
                    "line-height": "25px",
                    "border-bottom": "1px solid #e0e0e0",
                },
                ".ag-theme-balham .ag-header-cell-label": {
                    "font-size": "30px",
                    "font-weight": "600",
                    "padding": "5px 5px",  # Increased padding for taller header
                },
                ".ag-theme-balham .ag-header": {
                    "min-height": "500px",  # Increased header height
                },
                ".ag-theme-balham": {
                    "border": "1px solid #ddd",
                    "border-radius": "6px",
                    "overflow": "auto",
                },
                ".ag-root-wrapper": {
                    "border-radius": "8px",
                    "box-shadow": "0 2px 5px rgba(0,0,0,0.1)",
                    "max-height": "600px",
                    "overflow-y": "scroll",
                },
                ".ag-paging-panel": {
                    "display": "flex !important",
                    "justify-content": "center !important",
                    "font-size": "18px !important",
                    "padding": "10px !important",
                    "visibility": "visible !important",
                    "opacity": "1 !important",
                    "height": "auto !important",
                },
            }

            AgGrid(
                final_df[[x_col, y_col, 'No_of_Stocks', 'IndexDate', 'MonthEndDate']],
                gridOptions=grid_options,
                enable_enterprise_modules=False,
                theme="balham",
                height=None,  # Let CSS handle the height
                fit_columns_on_grid_load=False,  # Disable fit so scroll works
                allow_unsafe_jscode=True,
                custom_css=custom_css
            )
            
            
    
    def DateFiltersForCase1(self):
        if self.CaseClosetoClosedata is not None:
            st.markdown("<br><br><h2 style='text-align: center;'>📈 Stock-wise Close-to-Close % Change across Index Dates</h2>", unsafe_allow_html=True)

            start_dates = sorted(self.CaseClosetoClosedata['IdxStart_Date'].dropna().dt.date.unique())
            end_dates_full = sorted(self.CaseClosetoClosedata['IdxEnd_Date'].dropna().dt.date.unique())

            # CSS for inline layout & compact dropdowns
            st.markdown("""
                <style>
                    .inline-container {
                        display: flex;
                        flex-direction: row;
                        gap: 10px;
                        align-items: center;
                    }
                    .inline-container > div {
                        flex: 0 0 auto !important;
                        min-width: 20px;
                    }
                    div[data-baseweb="select"] {
                        width: 150px !important;
                    }
                    label[data-testid="stLabel"] > div {
                        display: none; /* Hide label space */
                    }
                    .date-label {
                        font-weight: 600;
                        margin-right: 5px;
                    }
                </style>
            """, unsafe_allow_html=True)

            # inline-container start
            st.markdown("""
                <style>
                    .row-flex {
                        display: flex;
                        flex-wrap: nowrap;
                        gap: 2px;
                        align-items: left;
                        margin-bottom: 15px;
                    }
                    .row-flex > div {
                        display: flex;
                        flex-direction: column;
                    }
                    .date-label {
                        font-weight: 600;
                        font-size: 14px;
                        margin-bottom: 2px;
                    }
                    div[data-baseweb="select"] {
                        width: 140px !important;
                    }
                </style>
            """, unsafe_allow_html=True)

            st.markdown('<div class="row-flex">', unsafe_allow_html=True)

            col1, col2, col3, col4 = st.columns(4, gap="small")  # very small Streamlit gap

            with col1:
                st.markdown('<span class="date-label"> Select Start Date :: </span>', unsafe_allow_html=True)
            with col2:
                start_date = st.selectbox("", start_dates, key="start_date", label_visibility="collapsed")
            with col3:
                st.markdown('<span class="date-label"> Select End Date :: </span>', unsafe_allow_html=True)
            with col4:
                valid_end_dates = [d for d in end_dates_full if d >= start_date]
                default_end_date = valid_end_dates[-1] if valid_end_dates else None
                end_date = st.selectbox(
                    "", 
                    valid_end_dates, 
                    index=valid_end_dates.index(default_end_date) if default_end_date else 0, 
                    key="end_date", 
                    label_visibility="collapsed"
                )


            st.markdown('</div><hr>', unsafe_allow_html=True)


            # Filter logic
            filtered_df = self.CaseClosetoClosedata[
                (self.CaseClosetoClosedata['IdxStart_Date'].dt.date >= start_date) &
                (self.CaseClosetoClosedata['IdxEnd_Date'].dt.date <= end_date)
            ]

            display_df = filtered_df.copy()
            display_df['IdxStart_Date'] = display_df['IdxStart_Date'].dt.date
            display_df['IdxEnd_Date'] = display_df['IdxEnd_Date'].dt.date

            st.markdown(f"<b>Records between {start_date} and {end_date}<b>", unsafe_allow_html=True)

            self.Case1ClosetoClose(start_date, end_date)



            
    def IndexSwingsFunc(self):
        if self.indexswingsdata is not None:
            self.index = self.indexswingsdata['Index'].iloc[0]
            st.markdown(f"<h2 style='text-align: center; color:#333333;'>📈 Cash Index Swings : {self.index}</h2>",unsafe_allow_html=True)
            
            column_order = ['Index', 'IndexDate', 'Type', 'IndexPrice', 'IndexClose', 'PercentChange', 'Days Difference', 'Time Difference String', 'Top to Top Days Only', 'Top to Top Time String']
            self.indexswingsdata = self.indexswingsdata[column_order]
            self.styled_dataframe(self.indexswingsdata)
            
           
    def Case1ClosetoClose(self, start_date, end_date):
        if self.CaseClosetoClosedata is not None:
            # Filter data for the given date range
            df = self.CaseClosetoClosedata[
                (self.CaseClosetoClosedata['IdxStart_Date'].dt.date >= start_date) & 
                (self.CaseClosetoClosedata['IdxEnd_Date'].dt.date <= end_date)
            ]

            if df.empty:
                st.warning("No data found in the selected date range.")
                return

            df['IdxStart_Date'] = df['IdxStart_Date'].dt.date
            df['IdxEnd_Date'] = df['IdxEnd_Date'].dt.date

            # Create 'DateRange' column
            df['DateRange'] = df['IdxStart_Date'].astype(str) + "To" + df['IdxEnd_Date'].astype(str) 
            print(df['DateRange'])

            # Merge TypeTorB and IndexChange into a single column
            df['Type_Index'] = df['TypeTorB'].astype(str) + " : % Change " + df['IndexChange'].astype(str) + " Close " + df['IndexClose'].astype(str)

            # Merge FirstClose and LastClose into "99 to 100" format
            df['CloseRange'] = df['FirstClose'].astype(str) + " to " + df['LastClose'].astype(str)

            # Ensure 'Symbol' and 'Index' pairs are maintained
            symbol_index = df[['Symbol', 'Index']].drop_duplicates(subset=['Symbol', 'Index']).set_index('Symbol')

            # Create pivot tables safely using pivot_table()
            type_index_pivot = df.pivot_table(index='Symbol', columns='DateRange', values='Type_Index', aggfunc='last')
            stock_pivot = df.pivot_table(index='Symbol', columns='DateRange', values='StockChange', aggfunc='last')
            close_pivot = df.pivot_table(index='Symbol', columns='DateRange', values='CloseRange', aggfunc='last')

            # Collect unique DateRanges
            date_ranges = df['DateRange'].unique()

            # Merge block-wise (IndexChange, StockChange & CloseRange)
            combined_pivots = []
            for date in date_ranges:
                combined_pivots.append(type_index_pivot[[date]].rename(columns={date: f"{date} (IndexChange)"}))
                combined_pivots.append(stock_pivot[[date]].rename(columns={date: f"{date} (StockChange)"}))
                combined_pivots.append(close_pivot[[date]].rename(columns={date: f"{date} (CloseRange)"}))  # FirstClose to LastClose

            # Merge all pivots
            merged_blocks = pd.concat(combined_pivots, axis=1)

            # Merge with symbol-index mapping
            merged = pd.concat([symbol_index, merged_blocks], axis=1).reset_index()

            # Ensure columns are properly ordered
            cols = ['Symbol', 'Index'] + [col for col in merged.columns if col not in ['Symbol', 'Index']]
            final_df = merged[cols]

            # Display the dataframe
            self.styled_dataframe(final_df)



    def MonthonMonthData(self):
        
        if self.month_high_data is not None:
            st.markdown("<br><h2 style='text-align: center;'>🔄 Month-on-Month Variation of Each Stock Between Swings</h2>", unsafe_allow_html=True)
            index_dates = self.month_high_data['IndexDate'].dropna().unique()
            index_dates_sorted = sorted(index_dates)
            selected_index_date = st.selectbox(
                "Index Date",
                index_dates_sorted,
                index=len(index_dates_sorted) - 2  # By default last date select hoga
            )

            df = self.month_high_data[self.month_high_data['IndexDate'] == selected_index_date].copy()
            if df.empty:
                st.warning("No data found for selected IndexDate.")
                return

            df.rename(columns={'PercentChange': 'IndexPercentChange'}, inplace=True)
            df['IndexDate to MonthEndDate'] = df['IndexDate'].astype(str) + " to " + df['MonthEndDate'].astype(str)
            pivot_df = df.pivot(index='Symbol', columns='IndexDate to MonthEndDate', values='StkPercChange').reset_index()
            meta_cols = ['Symbol','Index', 'IndexPrice', 'Type', 'IndexPercentChange', 'Days Difference',]
            meta_df = df[meta_cols].drop_duplicates(subset=['Symbol'])
            final_df = pd.merge(meta_df, pivot_df, on='Symbol', how='left')
            dynamic_cols = sorted([col for col in final_df.columns if 'to' in col])
            final_df = final_df[meta_cols + dynamic_cols]
            self.styled_dataframe(final_df)
            
        
    def RecoverStocksCombined(self, typeTorB):
        st.markdown(f"<br><h2 style='text-align: center;'>🚀📈 {typeTorB} Stock Recovery & Comprehensive Report</h2>", unsafe_allow_html=True)

        bottom_data = self.month_high_data[self.month_high_data['Type'] == typeTorB]
        index_dates = bottom_data['IndexDate'].dropna().unique()
        index_dates_sorted = sorted(index_dates)

        available_month_ends = []
        
        # CSS fix for vertical alignment
        st.markdown("""
            <style>
                .row-flex {
                    display: flex;
                    flex-wrap: nowrap;
                    gap: 10px;
                    align-items: center;
                    margin-bottom: 15px;
                }
                .row-flex > div {
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                }
                .date-label {
                    font-weight: 600;
                    font-size: 20px;
                    margin-bottom: 4px;
                    margin-left: 3px;
                }
                div[data-baseweb="select"] > div {
                    margin-top: -4px; /* shift dropdown upwards */
                }
                div[data-baseweb="select"] {
                    width: 160px !important;
                }
            </style>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="row-flex">', unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            st.markdown(f'<span class="date-label">Select Index Date ({typeTorB}):</span>', unsafe_allow_html=True)
        with col2:
            selected_index_date = st.selectbox(
                "", 
                index_dates_sorted, 
                index=len(index_dates_sorted) - 1
            )

        if selected_index_date:
            available_month_ends = bottom_data[bottom_data['IndexDate'] == selected_index_date]['MonthEndDate'].unique()
            available_month_ends_sorted = sorted(available_month_ends)

        with col3:
            st.markdown('<span class="date-label">Select Month End Date:</span>', unsafe_allow_html=True)
        with col4:
            selected_month_end = st.selectbox(
                "", 
                available_month_ends_sorted if selected_index_date else []
            )

        st.markdown('</div><hr>', unsafe_allow_html=True)

        if selected_index_date and selected_month_end:
            # Step 3: Filter CaseClosetoClosedata
            case_data_filtered = self.CaseClosetoClosedata[
                (self.CaseClosetoClosedata['IndexDate'] == selected_index_date)
            ]

            if case_data_filtered.empty:
                st.warning("No data found for this Index Date.")
                return
            
            if typeTorB == 'Bottom':

                ranges = [
                    (0, -10), (-10, -20), (-20, -30), (-30, -40), (-40, -50),
                    (-50, -60), (-60, -70), (-70, -80), (-80, -90), (-90, -100)
                ]

                summary_result = []
                detailed_rows = []

                for r in ranges:
                    range_data = case_data_filtered[
                        (case_data_filtered['StockChange'] < r[0]) & (case_data_filtered['StockChange'] >= r[1]) &
                        (case_data_filtered['StkSDate'] < selected_month_end)
                    ]
                    no_of_stocks = range_data['Symbol'].nunique()
                    avg_stock_change = range_data['StockChange'].mean()
                    
                    range_data['Symbol'] = range_data['Symbol'].str.strip().str.upper()
                    self.month_high_data['Symbol'] = self.month_high_data['Symbol'].str.strip().str.upper()
                    symbols_in_range = set(range_data['Symbol'].unique())
                    symbols_in_month_high = set(self.month_high_data[self.month_high_data['MonthEndDate'] == selected_month_end]['Symbol'].unique())

                    missing_symbols = symbols_in_range - symbols_in_month_high


                    month_data = self.month_high_data[
                        (self.month_high_data['MonthEndDate'] == selected_month_end) &
                        (self.month_high_data['Symbol'].isin(range_data['Symbol']))
                    ]
                                    
                    recovered_stocks = month_data[month_data['StkPercChange'] >= 0]
                    no_of_recovered = recovered_stocks['Symbol'].nunique()
                    avg_recovered_perc = recovered_stocks['StkPercChange'].mean()

                    month_data = month_data.merge(
                        range_data[['Symbol', 'StockChange', 'FirstClose', 'LastClose', 'IdxEnd_Date']],
                        on='Symbol', how='left', suffixes=('', '_CaseData')
                    )
                    month_data['100% Recovery Value'] = ((100 * month_data['StockChange'].abs()) / (100 + month_data['StockChange']))

                    recovered_100 = month_data[month_data['LastClose'] >= month_data['FirstClose']]
                    no_of_100_recovered = recovered_100['Symbol'].nunique()
                    names_100_recovered = ", ".join(recovered_100['Symbol'].unique()) if no_of_100_recovered > 0 else "-"

                    # Append to summary result
                    summary_result.append({
                        'Range': f"{r[0]} to {r[1]}",
                        'No of Stocks in Range': no_of_stocks,
                        'Range Stocks Avg %': round(avg_stock_change, 2) if not pd.isna(avg_stock_change) else None,
                        'No of Stocks Recovered': no_of_recovered,
                        'Avg % of Recovered': round(avg_recovered_perc, 2) if not pd.isna(avg_recovered_perc) else None,
                        'No of Stocks Recovered 100%': no_of_100_recovered,
                        '100% Recovered Stock Names': names_100_recovered,
                    })

                    # Append to detailed rows
                    for _, row in month_data.iterrows():
                        detailed_rows.append({
                            'Symbol': row['Symbol'],
                            'Range': f"{r[0]} to {r[1]}",
                            'StockChange': round(row['StockChange'], 2),
                            'StkPercChange': round(row['StkPercChange'], 2),
                            '100% Recovery': 'Yes' if row['LastClose'] >= row['FirstClose'] else 'No',
                            'FirstClose (Case)': row['FirstClose'],
                            'LastClose (Case)': row['LastClose_CaseData'],
                            'LastClose (MonthHigh)': row['LastClose']
                        })
                        
                        
            if typeTorB == 'Top':

                ranges = [
                    (0, 20), (20, 40), (40, 70),
                    (70, 100), (100, 300), (300, 1000), (1000, 10000000)
                ]

                summary_result = []
                detailed_rows = []

                for r in ranges:
                    range_data = case_data_filtered[
                        (case_data_filtered['StockChange'] > r[0]) & (case_data_filtered['StockChange'] <= r[1]) &
                        (case_data_filtered['StkSDate'] < selected_month_end)
                    ]
                    no_of_stocks = range_data['Symbol'].nunique()
                    avg_stock_change = range_data['StockChange'].mean()
                    
                    range_data['Symbol'] = range_data['Symbol'].str.strip().str.upper()
                    self.month_high_data['Symbol'] = self.month_high_data['Symbol'].str.strip().str.upper()
                    symbols_in_range = set(range_data['Symbol'].unique())
                    symbols_in_month_high = set(self.month_high_data[self.month_high_data['MonthEndDate'] == selected_month_end]['Symbol'].unique())

                    missing_symbols = symbols_in_range - symbols_in_month_high


                    month_data = self.month_high_data[
                        (self.month_high_data['MonthEndDate'] == selected_month_end) &
                        (self.month_high_data['Symbol'].isin(range_data['Symbol']))
                    ]
                                    
                    recovered_stocks = month_data[month_data['StkPercChange'] >= 0]
                    no_of_recovered = recovered_stocks['Symbol'].nunique()
                    avg_recovered_perc = recovered_stocks['StkPercChange'].mean()

                    month_data = month_data.merge(
                        range_data[['Symbol', 'StockChange', 'FirstClose', 'LastClose', 'IdxEnd_Date']],
                        on='Symbol', how='left', suffixes=('', '_CaseData')
                    )
                    month_data['100% Recovery Value'] = ((100 * month_data['StockChange'].abs()) / (100 + month_data['StockChange']))

                    recovered_100 = month_data[month_data['LastClose'] >= month_data['FirstClose']]
                    no_of_100_recovered = recovered_100['Symbol'].nunique()
                    names_100_recovered = ", ".join(recovered_100['Symbol'].unique()) if no_of_100_recovered > 0 else "-"

                    # Append to summary result
                    summary_result.append({
                        'Range': f"{r[0]} to {r[1]}",
                        'No of Stocks in Range': no_of_stocks,
                        'Range Stocks Avg %': round(avg_stock_change, 2) if not pd.isna(avg_stock_change) else None,
                        'No of Stocks not Fall': no_of_recovered,
                        'Avg % of not Fall': round(avg_recovered_perc, 2) if not pd.isna(avg_recovered_perc) else None,
                        
                    })

                    # Append to detailed rows
                    for _, row in month_data.iterrows():
                        detailed_rows.append({
                            'Symbol': row['Symbol'],
                            'Range': f"{r[0]} to {r[1]}",
                            'StockChange': round(row['StockChange'], 2),
                            'StkPercChange': round(row['StkPercChange'], 2),
                            # '100% Recovery': 'Yes' if row['LastClose'] >= row['FirstClose'] else 'No',
                            'FirstClose (Case)': row['FirstClose'],
                            'LastClose (Case)': row['LastClose_CaseData'],
                            'LastClose (MonthHigh)': row['LastClose']
                        })

            # Display both tables
            df_summary = pd.DataFrame(summary_result)
            st.markdown(f"<b>Range Records between {selected_index_date} and {selected_month_end}<b>", unsafe_allow_html=True)
            self.styled_dataframe(df_summary)

            st.download_button("Download Summary CSV", df_summary.to_csv(index=False).encode('utf-8'), f"{typeTorB}summary_recovery.csv", "text/csv")

            st.markdown(f"<hr><b>Stocks Records between {selected_index_date} and {selected_month_end}<b>", unsafe_allow_html=True)
            df_detailed = pd.DataFrame(detailed_rows)
            self.styled_dataframe(df_detailed)

            st.download_button("Stocks Detailed CSV", df_detailed.to_csv(index=False).encode('utf-8'), f"{typeTorB}detailed_recovery.csv", "text/csv")
            
            
   





    def run(self):
        # st.set_page_config(
        #     page_title="Cash Data Analysis",
        #     layout="wide",
        #     initial_sidebar_state="expanded"
        # )
        st.markdown("""
            <style>
                body { 
                    overflow: hidden !important;  /* ✅ Poore page ka scroll remove */
                }
                .block-container {
                    padding: 10px !important;  /* ✅ Extra padding remove */
                }
                .centered-box {
                    width: 100% !important;
                    max-width: 1000px;  /* ✅ Max width fix */
                    margin: auto;
                    text-align: center;
                }
            </style>
        """, unsafe_allow_html=True)

        # st.sidebar.markdown("## ⚙️ Filters")

        # self.select_csv_file()  # sidebar ke andar bhi move kar sakte ho if needed
        
        # with st.spinner('Reading CSV...'):
        #     self.read_csv()
            
        st.markdown(
            """
            <style>
                .centered-box {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100px;  /* ✅ Box ki height fix */
                    width: 90%;  /* ✅ Page ke end tak thodi chhoti width */
                    margin: auto; /* ✅ Box center align hoga */
                    background-color: #f0f2f6;
                    border-radius: 20px;
                    box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
                    font-size: 35px; /* ✅ Font ko responsive size diya */
                    font-weight: bold;
                    color: #154406;
                    text-align: center;
                }
            </style>

            <div class="centered-box">
                🎯📊 Cash Market Insights: Tracking Stock Trends & Movements
            </div>
            <br><hr>
            """,
            unsafe_allow_html=True
        )
        

        # Display Centered Box with Heading
        # st.markdown('<div class="centered-box">🎯📊 Cash Market Insights: Tracking Stock Trends & Movements</div><br><hr>', unsafe_allow_html=True)
        
        # self.upload() 
        self.load()
        # self.IndexSwingsFunc()
        # self.DateFiltersForCase1()
        # self.MonthonMonthData()
        # # self.RecoverStocksCombinedTop()
        # self.RecoverStocksCombined('Bottom')
        # self.RecoverStocksCombined('Top')
        # self.RecoveryAnalysis()
        # self.IndexDatePivot()
        

        
if __name__ == "__main__":
    app = CSVReaderApp()
    app.run()
