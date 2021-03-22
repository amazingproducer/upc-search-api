#!/usr/bin/env python3.9
from fastapi import FastAPI, Path, Request, Body
from starlette.responses import JSONResponse, RedirectResponse
import databases
from sqlalchemy import ARRAY, CheckConstraint, Column, Date, Enum, Index, Integer, Numeric, String, Text, UniqueConstraint, text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from enum import Enum as En
from os import getenv
from typing import Optional
upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')

DB_URL = f"postgresql://barcodeserver:{upc_DATABASE_KEY}@10.8.0.55/upc_data"

database = databases.Database(DB_URL)

Base = declarative_base()
metadata = Base.metadata


class DatasetSourceMeta(Base):
    __tablename__ = 'dataset_source_meta'

    id = Column(Integer, primary_key=True, server_default=text("nextval('dataset_source_meta_id_seq'::regclass)"))
    source_name = Column(Enum('usda', 'uhtt', 'off', name='upc_data_source'))
    refresh_check_url = Column(Text, nullable=False)
    current_version_hash = Column(Text)
    current_version_url = Column(Text)
    current_version_release_name = Column(Text)
    current_version_date = Column(Date)
    last_update_check = Column(Date)


class ProductInfo(Base):
    __tablename__ = 'product_info'
    __table_args__ = (
        CheckConstraint("(upc)::text ~ '^[0-9]*$'::text"),
        CheckConstraint('length((upc)::text) >= 12'),
        UniqueConstraint('upc', 'source'),
        Index('idx_product_info', 'source', 'upc', 'source_item_publication_date')
    )

    id = Column(Integer, primary_key=True, server_default=text("nextval('product_info_id_seq'::regclass)"))
    source = Column(Enum('usda', 'uhtt', 'off', name='upc_data_source'))
    source_item_id = Column(Text)
    upc = Column(String(14), nullable=False, index=True)
    name = Column(Text, nullable=False)
    category = Column(ARRAY(Text()))
    db_entry_date = Column(Date, nullable=False)
    source_item_submission_date = Column(Date)
    source_item_publication_date = Column(Date)
    serving_size = Column(Numeric)
    serving_size_unit = Column(Text)
    serving_size_fulltext = Column(Text)

engine = create_engine(DB_URL)


class DataSource(str, En):
    USDA = "usda"
    UHTT = "uhtt"
    OpenFoodFacts = "off"

class UPCNotFoundException(Exception):
    def __init__(self, barcode: str):
        self.barcode = barcode

api = FastAPI()
#api = FastAPI(openapi_url="/api/v2/openapi.json", docs_url="/api/v2/docs")

def expand_barcode(code):
    q_barcode = code
    while len(q_barcode) < 14:
        q_barcode = "0"+q_barcode
    return q_barcode

def get_source_name(s_value):
    for i in DataSource:
        if i.value == s_value:
            return i.name

def mutate_result(result):
    if isinstance(result, list):
        lr = []
        for i in range(len(DataSource)-1):
            try:
                if result[i]:
                    lr.append(dict(result[i]))
            except:
                pass
        return lr
    else:
        return dict(result)

@api.on_event("startup")
async def db_connect():
    await database.connect()

@api.on_event("shutdown")
async def db_disconnect():
    await database.disconnect()

@api.exception_handler(UPCNotFoundException)
async def UPCNotFound_exception_handler(request: Request, exc: UPCNotFoundException):
    return JSONResponse(
        status_code=404,
        content={
            "upc":exc.barcode,
            "error":"no entry found."
        }
    )

@api.get('/', include_in_schema = False)
def root():
#    response = RedirectResponse(url='/api/v2/docs')
    response = RedirectResponse(url='/docs')
    return response

@api.get("/name/{barcode}")
async def get_name_by_barcode(source:Optional[DataSource] = None, barcode: str = Path(..., min_length= 12, max_length=14, regex=r"^\d+$")):
    results = None
    if barcode:
        q_barcode = expand_barcode(barcode)
        query = "SELECT upc, name, source from product_info where upc = :barcode"
        if source:
            print(source.value)
            query = query + " and source = :source"
            results = await database.fetch_one(query=query, values={"barcode":q_barcode, "source":source.value})
            if results:
                res = mutate_result(results)
                res["upc"] = barcode
                res["source"] = source.name
                return res
        results = await database.fetch_all(query=query, values={"barcode":q_barcode})
        if results:
            res = mutate_result(results)
            for r in res:
                r["upc"] = barcode
                r["source"] = get_source_name(r["source"])
            return res
        raise UPCNotFoundException(barcode=barcode)

@api.get("/grocy/{barcode}")
async def get_grocy_data_by_barcode(barcode:str=Path(..., min_length= 12, max_length=14, regex=r"^\d+$")):
    results = None
    if barcode:
        q_barcode = expand_barcode(barcode)
        query = "SELECT * from product_info where upc = :barcode"
        results = await database.fetch_all(query=query, values={"barcode":q_barcode})
        if results:
            lr = []
            for i in range(len(DataSource)-1):
                try:
                    if results[i]:
                        lr.append(dict(results[i]))
                except:
                    pass
            for res in lr:
                del res["id"]
                del res["source_item_id"]
                res["upc"] = barcode
                # if len(barcode) == 14:
                #     res["gtin-14"] = res.pop("upc")
                # elif len(barcode) == 13:
                #     res["ean-13"] = res.pop("upc")
                res["product_name"] = res.pop("name")
                if res["source"] == "usda":
                    del res["source"]
                    return res
                if res["source"] == "uhtt":
                    del res["source"]
                    return res
            del lr[0]["source"]
            return lr[0]
        raise UPCNotFoundException(barcode=barcode)
