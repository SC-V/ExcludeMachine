import datetime
import shapely
from shapely.geometry import Point, Polygon
import requests
import json
import pandas
from pytz import timezone
import io
import streamlit as st
import pydeck as pdk
import dateutil.parser

st.set_page_config(layout="wide")

CLAIM_SECRETS = st.secrets["CLAIM_SECRETS"]
API_URL = st.secrets["API_URL"]
FILE_BUFFER_ALL = io.BytesIO()
FILE_BUFFER_EXC = io.BytesIO()

SDD_ZONE_LINK = r"https://raw.githubusercontent.com/SC-V/ExcludeMachine/main/sdd.json"
sdd_geometry = json.loads(open('sdd.json').read())
sdd_polygon: Polygon = shapely.geometry.shape(sdd_geometry)

NDD_NEAR_ZONE_LINK = r"https://raw.githubusercontent.com/SC-V/ExcludeMachine/main/ndd_near.json"
ndd_near_geometry = json.loads(open('ndd_near.json').read())
ndd_near_polygon: Polygon = shapely.geometry.shape(ndd_near_geometry)

NDD_FAR_ZONE_LINK = r"https://raw.githubusercontent.com/SC-V/ExcludeMachine/main/ndd_far.json"
ndd_far_geometry = json.loads(open('ndd_far.json').read())
ndd_far_polygon: Polygon = shapely.geometry.shape(ndd_far_geometry)


def check_for_zones(row):
    row["sdd_zone"] = sdd_polygon.contains(Point([row["lon"], row["lat"]]))
    row["near_ndd_zone"] = ndd_near_polygon.contains(Point([row["lon"], row["lat"]]))
    row["far_ndd_zone"] = ndd_far_polygon.contains(Point([row["lon"], row["lat"]]))
    return row


def get_claims(secret, date_from, date_to, cursor=0):
    url = API_URL
    timezone_offset = "-04:00"
    payload = json.dumps({
        "created_from": f"{date_from}T00:00:00{timezone_offset}",
        "created_to": f"{date_to}T23:59:59{timezone_offset}",
        "limit": 1000,
        "cursor": cursor
    }) if cursor == 0 else json.dumps({"cursor": cursor})

    headers = {
        'Content-Type': 'application/json',
        'Accept-Language': 'en',
        'Authorization': f"Bearer {secret}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    claims = json.loads(response.text)
    cursor = None
    try:
        cursor = claims['cursor']
        print(f"CURSOR: {cursor}")
    except:
        print("LAST PAGE PROCESSED")
    try:
        return claims['claims'], cursor
    except:
        return [], None


def get_report(option="Today", start_=None, end_=None) -> pandas.DataFrame:
    offset_back = 0
    if option == "Yesterday":
        offset_back = 1
    elif option == "Tomorrow":
        offset_back = -1
    elif option == "Received":
        offset_back = 0

    client_timezone = "America/Santiago"

    if option == "Monthly":
        start_ = "2023-04-15"
        end_ = "2023-05-31"
        today = datetime.datetime.now(timezone(client_timezone))
        date_from_offset = datetime.datetime.fromisoformat(start_).astimezone(
            timezone(client_timezone)) - datetime.timedelta(days=1)
        date_from = date_from_offset.strftime("%Y-%m-%d")
        date_to = end_
    elif option == "Weekly":
        start_ = "2023-05-08"
        end_ = "2023-05-14"
        today = datetime.datetime.now(timezone(client_timezone))
        date_from_offset = datetime.datetime.fromisoformat(start_).astimezone(
            timezone(client_timezone)) - datetime.timedelta(days=1)
        date_from = date_from_offset.strftime("%Y-%m-%d")
        date_to = end_
    elif option == "Received":
        today = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=offset_back)
        search_from = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=7)
        search_to = today.replace(hour=23, minute=59, second=59, microsecond=999999) + datetime.timedelta(days=2)
        date_from = search_from.strftime("%Y-%m-%d")
        date_to = search_to.strftime("%Y-%m-%d")
    else:
        today = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=offset_back)
        search_from = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=2)
        search_to = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        date_from = search_from.strftime("%Y-%m-%d")
        date_to = search_to.strftime("%Y-%m-%d")

    today = today.strftime("%Y-%m-%d")
    report = []
    for secret in CLAIM_SECRETS:
        claims, cursor = get_claims(secret, date_from, date_to)
        while cursor:
            new_page_claims, cursor = get_claims(secret, date_from, date_to, cursor)
            claims = claims + new_page_claims
        print(f"{datetime.datetime.now()}: Processing {len(claims)} claims")
        for claim in claims:
            try:
                claim_from_time = claim['same_day_data']['delivery_interval']['from']
            except:
                continue
            cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone(client_timezone))
            cutoff_date = cutoff_time.strftime("%Y-%m-%d")
            if not start_ and option != "Received":
                if cutoff_date != today:
                    continue
            report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
            try:
                report_client_id = claim['route_points'][1]['external_order_id']
            except:
                report_client_id = "External ID not set"
            report_claim_id = claim['id']
            try:
                report_lo_code = claim['items'][0]['extra_id']
            except:
                report_lo_code = "No LO code"
            report_receiver_address = claim['route_points'][1]['address']['fullname']
            report_receiver_phone = claim['route_points'][1]['contact']['phone']
            report_receiver_name = claim['route_points'][1]['contact']['name']
            try:
                report_comment = claim['comment']
            except:
                report_comment = "Missing comment in claim"
            report_status = claim['status']
            report_created_time = dateutil.parser.isoparse(claim['created_ts']).astimezone(timezone(client_timezone))
            report_status_time = dateutil.parser.isoparse(claim['updated_ts']).astimezone(timezone(client_timezone))
            report_longitude = claim['route_points'][1]['address']['coordinates'][0]
            report_latitude = claim['route_points'][1]['address']['coordinates'][1]
            report_store_longitude = claim['route_points'][0]['address']['coordinates'][0]
            report_store_latitude = claim['route_points'][0]['address']['coordinates'][1]
            report_corp_id = claim['corp_client_id']
            row = [report_cutoff, report_created_time, report_client_id, report_claim_id, report_lo_code, report_status,
                   report_status_time, report_receiver_address, report_receiver_phone, report_receiver_name,
                   report_comment, report_longitude, report_latitude, report_store_longitude, report_store_latitude, report_corp_id]
            report.append(row)

    print(f"{datetime.datetime.now()}: Building dataframe")
    result_frame = pandas.DataFrame(report,
                                    columns=["cutoff", "created_time", "client_id", "claim_id", "lo_code", "status",
                                             "status_time", "receiver_address", "receiver_phone",
                                             "receiver_name", "client_comment", "lon", "lat", "store_lon",
                                             "store_lat", "corp_client_id"])
    result_frame = result_frame.apply(lambda row: check_for_zones(row), axis=1)
    print(f"{datetime.datetime.now()}: Constructed dataframe")
    return result_frame


option = "Received"


@st.cache_data(ttl=3600.0)
def get_cached_report(option):
    report = get_report(option)
    return report


client_timezone = "America/Santiago"
TODAY = datetime.datetime.now(timezone(client_timezone)).strftime("%Y-%m-%d") \
    if option == "Today" \
    else datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=1)

df = get_cached_report("Received")
filtered_frame = df[df['status'].isin(["performer_lookup"])]

st.markdown(f"# Exclude machine")

if st.button("Reload data", type="primary"):
    st.cache_data.clear()

col_metric_1, _, col_metric_2, col_metric_3, _ = st.columns(5)

with col_metric_1:
    delivery_type = st.selectbox("Delivery type", ["SDD", "NDD near", "NDD far"])

if delivery_type == "SDD":
    orders_out_of_zone = filtered_frame[filtered_frame['sdd_zone'].isin([False])][["claim_id", "created_time", "client_id", "receiver_address"]]
elif delivery_type == "NDD near":
    orders_out_of_zone = filtered_frame[filtered_frame['near_ndd_zone'].isin([False])][["claim_id", "created_time", "client_id", "receiver_address"]]
else:
    orders_out_of_zone = filtered_frame[filtered_frame['far_ndd_zone'].isin([False])][["claim_id", "created_time", "client_id", "receiver_address"]]

number_of_claims = len(filtered_frame)
number_of_excludes = len(orders_out_of_zone)
exclusion_rate = number_of_excludes / number_of_claims * -1
col_metric_2.metric(f"Claims", number_of_claims)
col_metric_3.metric(f"Out of zone 🔥", number_of_excludes, delta=f"{exclusion_rate:.0%}")

with st.expander("📋 All received orders", expanded=False):
    st.dataframe(filtered_frame)

with st.expander("🌎 Orders on the map", expanded=False):
    chart_data_delivered = filtered_frame[filtered_frame["status"].isin(['delivered', 'delivered_finish'])]
    chart_data_in_delivery = filtered_frame[~filtered_frame["status"].isin(
        ['delivered', 'delivered_finish', 'cancelled', 'cancelled_by_taxi', 'returning', 'returned_finish',
         'return_arrived'])]
    chart_data_returns = filtered_frame[
        filtered_frame["status"].isin(['returning', 'returned_finish', 'return_arrived'])]
    chart_data_cancels = filtered_frame[filtered_frame["status"].isin(['cancelled', 'cancelled_by_taxi'])]
    view_state_lat = filtered_frame['lat'].iloc[0]
    view_state_lon = filtered_frame['lon'].iloc[0]

    geojson_sdd_area = pdk.Layer(
        'GeoJsonLayer',
        SDD_ZONE_LINK,
        opacity=0.1,
        stroked=False,
        filled=True,
        extruded=False,
        wireframe=True,
        get_fill_color='[223, 30, 38]',
        get_line_color='[223, 30, 38]',
        pickable=False
    )

    print(geojson_sdd_area)

    geojson_near_ndd_area = pdk.Layer(
        'GeoJsonLayer',
        NDD_NEAR_ZONE_LINK,
        opacity=0.1,
        stroked=False,
        filled=True,
        extruded=False,
        wireframe=True,
        get_fill_color='[243, 114, 32]',
        get_line_color='[243, 114, 32]',
        pickable=False
    )

    geojson_far_ndd_area = pdk.Layer(
        'GeoJsonLayer',
        NDD_FAR_ZONE_LINK,
        opacity=0.1,
        stroked=False,
        filled=True,
        extruded=False,
        wireframe=True,
        get_fill_color='[255, 213, 0]',
        get_line_color='[255, 213, 0]',
        pickable=False
    )

    st.pydeck_chart(pdk.Deck(
        map_style=None,
        height=1000,
        initial_view_state=pdk.ViewState(
            latitude=view_state_lat,
            longitude=view_state_lon,
            zoom=10,
            pitch=0,
        ),
        tooltip={"text": "{cutoff}\n{client_id} : {claim_id}"},
        layers=[
            geojson_sdd_area,
            geojson_near_ndd_area,
            geojson_far_ndd_area,
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_in_delivery,
                get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]',
                get_radius=300,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_cancels,
                get_position='[lon, lat]',
                get_color='[215, 210, 203, 200]',
                get_radius=300,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_returns,
                get_position='[lon, lat]',
                get_color='[237, 139, 0, 160]',
                get_radius=300,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=filtered_frame,
                get_position=[-70.6945098, -33.3688048],
                get_color='[0, 128, 255, 160]',
                get_radius=400,
                pickable=True
            )
        ],
    ))

with st.expander("🔥 Exclude those orders", expanded=False):
    st.dataframe(orders_out_of_zone)

print(f"{datetime.datetime.now()}: Rendering download button")
with pandas.ExcelWriter(FILE_BUFFER_ALL, engine='xlsxwriter') as writer:
    filtered_frame["status_time"] = filtered_frame["status_time"].apply(
        lambda a: pandas.to_datetime(a).date()).reindex()
    filtered_frame["created_time"] = filtered_frame["created_time"].apply(
        lambda a: pandas.to_datetime(a).date()).reindex()
    filtered_frame.to_excel(writer, sheet_name='wh_routes_report')
    writer.close()

    st.download_button(
        label="Download all orders",
        data=FILE_BUFFER_ALL,
        file_name=f"route_report_{TODAY}.xlsx",
        mime="application/vnd.ms-excel"
    )

with pandas.ExcelWriter(FILE_BUFFER_EXC, engine='xlsxwriter') as writer:
    orders_out_of_zone["created_time"] = orders_out_of_zone["created_time"].apply(
        lambda a: pandas.to_datetime(a).date()).reindex()
    orders_out_of_zone.to_excel(writer, sheet_name='wh_exclude_report')
    writer.close()

    st.download_button(
        label="Download excluded orders",
        data=FILE_BUFFER_EXC,
        file_name=f"excluded_orders_{TODAY}.xlsx",
        mime="application/vnd.ms-excel"
    )

st.caption(f"Use responsibly.")

print(f"{datetime.datetime.now()}: Finished")
