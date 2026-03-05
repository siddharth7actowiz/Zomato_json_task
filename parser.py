from parsel import Selector
import json
import re
import config
from uitls import *
from dbconfig import *
import time

def parser(raw_data):
    parsed_data = {}
    selector = Selector(raw_data)
    page_info=selector.jmespath("page_info")
    parsed_data["res_id"]=page_info.jmespath("resId").get()
    parsed_data["res_name"]=page_info.jmespath("ogTitle").get()
    parsed_data["res_website"]=page_info.jmespath("canonicalUrl").get()

    parsed_data["restaurant_contact"] = selector.jmespath("page_data.sections.SECTION_RES_CONTACT.phoneDetails.phoneStr").get()
    parsed_data["full_address"] = selector.jmespath("page_data.sections.SECTION_RES_CONTACT.address").get()

    region=selector.jmespath("page_data.sections.SECTION_RES_HEADER_DETAILS.LOCALITY.text").get()
    new_reg=region.replace(", Ahmedabad"," ").strip()

    parsed_data["region"] =new_reg

    parsed_data["city"] = selector.jmespath("page_data.sections.SECTION_RES_CONTACT.city_name" ).get()

    parsed_data["pincode"] = selector.jmespath("page_data.sections.SECTION_RES_CONTACT.zipcode").get()

    parsed_data["state"] = "Gujarat"
    lic_text = selector.jmespath(
        "page_data.order.menuList.fssaiInfo.text"
    ).get()
    lic_no=re.search(r"\d+",lic_text).group()
    
    parsed_data["licence_no"] =lic_no  
    parsed_data[    "open_time"]="12pm"
    parsed_data["close_time"]= "11pm"
    parsed_data["state"] = "Gujarat"

    cuisions=selector.jmespath("page_data.sections.SECTION_RES_HEADER_DETAILS.CUISINES").getall()
   
    cuisines_data = []

    for cuisine in cuisions:
        cuisines_data.append({
            "name": cuisine.get("name"),
            "url": cuisine.get("url")
        })
    parsed_data["Cuisions"]=json.dumps(cuisines_data) #before storing to database making sure to convert it json str    
    
        
    menus = selector.jmespath("page_data.order.menuList.menus").getall()

    items_data = []

    for menu in menus:
        print()
        main_cat_id = menu.get("menu",{}).get("id")
        main_cat_name =menu.get("menu",{}).get("name")
       
        
        for cat in menu.get("menu", {}).get("categories", []):

            category = cat.get("category", {})
            sub_cat_name = category.get("name")

            for item_wrap in category.get("items", []):

                item = item_wrap.get("item", {})

                items_data.append({
                    "main_cat_id": main_cat_id,
                    "main_cat_name": main_cat_name,
                    "sub_category": sub_cat_name or main_cat_name,
                    "item_id": item.get("id"),
                    "item_name": item.get("name"),
                    "description": item.get("desc") or sub_cat_name if sub_cat_name else main_cat_name})
   
    # before storing to database/ passing to db function making sure to convert it json str beacuse mysql is not able to convert list[dict] to str conversion
    parsed_data["menu_items"] = json.dumps(items_data)
    return parsed_data
   

def main():
    raw_data = read_json_file(config.FILE_PATH)
    create_table(config.TABLE_NAME)
    for raw in raw_data:
        data= parser(json.dumps(raw))
        insert(config.TABLE_NAME,data)

if __name__ == '__main__': #provides encapculation ,modularity ,File can act as standalonr script as well as module
    st = time.time()
    main()
    tt = time.time() - st
    print(tt)

