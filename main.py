from fastapi import FastAPI, Path
from starlette.responses import RedirectResponse
import databases
from sqlalchemy import ARRAY, CheckConstraint, Column, Date, Enum, Index, Integer, Numeric, String, Text, UniqueConstraint, text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from os import getenv

upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')

DB_URL = f"postgresql://barcodeserver:{upc_DATABASE_KEY}@tengu.i.shamacon.us/upc_data"

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


api = FastAPI()

@api.on_event("startup")
async def db_connect():
    await database.connect()

@api.on_event("shutdown")
async def db_disconnect():
    await database.disconnect()

@api.get('/')
async def root():
    response = RedirectResponse(url='/docs')
    return response 


@api.get("/name/{barcode}")
async def get_name_by_barcode(barcode: str = Path(..., min_length= 12, max_length=14, regex=r"^\d+$")):
    results = {"UPC": None}
    if barcode:
        query = "SELECT name from product_info where upc = :barcode"
        results = await database.fetch_all(query=query, values={"barcode":barcode})
    return results
