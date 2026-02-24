import json
import re
from datetime import *

structured_data = {}
cuisine_dict = {}

raw_json = "D:\\Siddharth\\JSON\\input_zomato_data.json"

f_name = "ZOMATO"
today = datetime.today()
todays_date = datetime.strftime(today, "%Y_%m_%d")
file_name = f"{f_name}_{todays_date}.json"


def read_json(raw_json):
    with open(raw_json, "r", encoding="utf-8") as file:
        return json.load(file)


def json_parser(raw_dict):

    global structured_data, cuisine_dict

    base_path = raw_dict.get("page_info")
    contact_section = raw_dict["page_data"]["sections"]["SECTION_RES_CONTACT"]

 #basic detalis

    structured_data["restaurant_id"] = base_path.get("resId")
    structured_data["restaurant_name"] = base_path.get("ogTitle")
    structured_data["restaurant_contact"] = contact_section["phoneDetails"]["phoneStr"]

    structured_data["full_address"] = contact_section.get("address")
    structured_data["region"] = contact_section.get("country_name")
    structured_data["city"] = contact_section.get("city_name")
    structured_data["pincode"] = contact_section.get("zipcode")
    structured_data["state"] = "Gujarat"



    LIC_NO = raw_dict.get("page_data").get("order").get("menuList").get("fssaiInfo").get("text")
    match = re.search(r'(\d+)', LIC_NO)
    structured_data["fssai_licence_number"] = int(match.group()) if match else None


    # Timing


    timing = raw_dict["page_data"]["sections"]["SECTION_BASIC_INFO"]["timing"]["customised_timings"]["opening_hours"][0]["timing"]

    open_time = timing.split()[0]
    close_time = timing.split()[-1]

    if open_time.lower() in ["12noon", "noon"]:
        open_time = "12pm"

    structured_data["open_time"] = open_time
    structured_data["close_time"] = close_time

    #cuisions

    cuisine_data = raw_dict["page_data"]["sections"]["SECTION_RES_HEADER_DETAILS"]["CUISINES"]

    cuisine_counter = 1

    for cuisine in cuisine_data:

        cuisine_dict[f"cuisine_{cuisine_counter}_restaurant_id"] = structured_data["restaurant_id"]
        cuisine_dict[f"cuisine_{cuisine_counter}_name"] = cuisine.get("name")
        cuisine_dict[f"cuisine_{cuisine_counter}_url"] = cuisine.get("url")

        cuisine_counter += 1

    cuisine_dict["total_cuisines"] = cuisine_counter - 1

   #meni_items-

    item_counter = 1

    for menu in raw_dict["page_data"]["order"]["menuList"]["menus"]:
        category_name = menu["menu"]["name"]

        for category in menu["menu"]["categories"]:
            for item in category["category"]["items"]:

                item_data = item["item"]

                structured_data[f"item_{item_counter}_category"] = category_name
                structured_data[f"item_{item_counter}_id"] = item_data["id"]
                structured_data[f"item_{item_counter}_name"] = item_data["name"]
                structured_data[f"item_{item_counter}_description"] = item_data["desc"]
                structured_data[f"item_{item_counter}_slugs"] = ",".join(item_data["tag_slugs"])
                structured_data[f"item_{item_counter}_is_veg"] = (
                    True if item_data["dietary_slugs"][0] == "veg" else False
                )

                item_counter += 1

    structured_data["total_items"] = item_counter - 1

    return structured_data, cuisine_dict


def export_structured_data_func(rest_data, cuisine_data):
    with open(file_name, "w", encoding="utf-8") as file:
        json.dump({
            "restaurant_data": rest_data,
            "cuisine_data": cuisine_data
        }, file, indent=4)



# FUNCTION CALLS


raw_dict = read_json(raw_json)
restaurant_data, cuisines_data = json_parser(raw_dict)
export_structured_data_func(restaurant_data, cuisines_data)