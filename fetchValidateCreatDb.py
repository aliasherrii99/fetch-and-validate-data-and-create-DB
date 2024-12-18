import requests
from pydantic import BaseModel , Field
from typing import Optional
import json
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column , Integer, String , BigInteger , Float , Boolean , DateTime



# 1- get data
url = 'https://fund.fipiran.ir/api/v1/fund/fundcompare'
response = requests.get(url)
data = json.loads(response.text)

# 2- validate data
class DataV(BaseModel):
    reg_no: int = Field(alias="regNo")
    name: str
    rank_of_12_month: Optional[int] = Field(alias="rank0f12Month", default=None)
    rank_of_24_month: Optional[int] = Field(alias="rank0f24Month", default=None)
    rank_of_36_month: Optional[int] = Field(alias="rank0f36Month", default=None)
    rank_of_48_month: Optional[int] = Field(alias="rank0f48Month", default=None)
    rank_of_60_month: Optional[int] = Field(alias="rank0f60Month", default=None)
    rank_last_update: str = Field(alias="rankLastUpdate")
    fund_type: int = Field(alias="fundType")
    type_of_invest: str = Field(alias="typeOfInvest")
    fund_size: Optional[int] = Field(alias="fundSize", default=None)
    initiation_date: str = Field(alias="initiationDate")
    daily_efficiency: Optional[float] = Field(alias="dailyEfficiency", default=None)
    weekly_efficiency: Optional[float] = Field(alias="weeklyEfficiency", default=None)
    monthly_efficiency: Optional[float] = Field(alias="monthlyEfficiency", default=None)
    quarterly_efficiency: Optional[float] = Field(alias="quarterlyEfficiency", default=None)
    six_month_efficiency: Optional[float] = Field(alias="sixMonthEfficiency", default=None)
    annual_efficiency: Optional[float] = Field(alias="annualEfficiency", default=None)
    statistical_nav: Optional[float] = Field(alias="statisticalNav", default=None)
    efficiency: Optional[float] = Field(alias="efficiency", default=None)
    cancel_nav: Optional[float] = Field(alias="cancelNav", default=None)
    issue_nav: Optional[float] = Field(alias="issueNav", default=None)
    dividend_interval_period: Optional[int] = Field(alias="dividendIntervalPeriod", default=None)
    guaranteed_earning_rate: Optional[float] = Field(alias="guaranteedEarningRate", default=None)
    data: Optional[str] = Field(alias="data", default=None)
    net_asset: Optional[int] = Field(alias="netAsset", default=None)
    estimated_earning_rate: Optional[float] = Field(alias="estimatedEarningRate", default=None)
    invested_units: Optional[int] = Field(alias="investedUnits", default=None)
    articles_of_association_link: Optional[str] = Field(alias="articlesOfAssociationLink", default=None)
    prospectus_link: Optional[str] = Field(alias="prosoectusLink", default=None)
    website_address: list = Field(alias='websiteAddress')
    manager: str
    manager_seo_register_no: Optional[int] = Field(alias="managerSeoRegisterNo", default=None)
    guarantor_seo_register_no: Optional[int] = Field(alias="guarantorSeoRegisterNo", default=None)
    auditor: str
    custodian: str
    guarantor: str
    beta: Optional[float] = Field(alias="beta", default=None)
    alpha: Optional[float] = Field(alias="alpha", default=None)
    is_completed: bool = Field(alias='isCompleted')
    five_best: Optional[float] = Field(alias="fiveBest", default=None)
    stock: Optional[float] = Field(alias="stock", default=None)
    bond: Optional[float] = Field(alias="bond", default=None)
    other: Optional[float] = Field(alias="other", default=None)
    cash: Optional[float] = Field(alias="cash", default=None)
    deposit: Optional[float] = Field(alias="deposit", default=None)
    fund_unit: Optional[float] = Field(alias="fundUnit", default=None)
    commodity: Optional[float] = Field(alias="commodity", default=None)
    fund_publisher: int = Field(alias="fundPublisher")
    small_symbol_name: Optional[str] = Field(alias="smallSymbolName", default=None)
    ins_code: Optional[str] = Field(alias="insCode", default=None)
    fund_watch: Optional[str] = Field(alias="fundWatch", default=None)

data_list = []
for i in range(0,len(data['items'])):
    data_list.append(DataV.model_validate(data['items'][i]))


# # 3- create table
eg = create_engine('postgresql://postgres:09101489397@localhost:5432/postgres')
Base = declarative_base()
Se = sessionmaker(bind=eg)
se = Se()

class CreateDb(Base):
    __tablename__ = 'dataBors'
    reg_no = Column(BigInteger, primary_key=True)
    #
    name = Column(String, nullable=False)
    rank_of_12_month = Column(Float, nullable=True)
    rank_of_24_month = Column(Float, nullable=True)
    rank_of_36_month = Column(Float, nullable=True)
    rank_of_48_month = Column(Float, nullable=True)
    rank_of_60_month = Column(Float, nullable=True)
    rank_last_update = Column(DateTime, nullable=True)
    fund_type = Column(Integer, nullable=False)
    type_of_invest = Column(String, nullable=False)
    fund_size = Column(BigInteger, nullable=True)
    initiation_date = Column(DateTime, nullable=False)
    daily_efficiency = Column(Float, nullable=True)
    weekly_efficiency = Column(Float, nullable=True)
    monthly_efficiency = Column(Float, nullable=True)
    quarterly_efficiency = Column(Float, nullable=True)
    six_month_efficiency = Column(Float, nullable=True)
    annual_efficiency = Column(Float, nullable=True)
    statistical_nav = Column(Float, nullable=True)
    efficiency = Column(Float, nullable=True)
    cancel_nav = Column(Float, nullable=True)
    issue_nav = Column(Float, nullable=True)
    dividend_interval_period = Column(BigInteger, nullable=True)
    #
    guaranteed_earning_rate = Column(Float, nullable=True)
    data = Column(String, nullable=True)
    net_asset = Column(BigInteger, nullable=True)
    estimated_earning_rate = Column(Float, nullable=True)
    invested_units = Column(BigInteger, nullable=True)
    articles_of_association_link = Column(String, nullable=True)
    prospectus_link = Column(String, nullable=True)
    website_address = Column(String, nullable=True)
    manager = Column(String, nullable=False)
    manager_seo_register_no = Column(BigInteger, nullable=True)
    #
    guarantor_seo_register_no = Column(BigInteger, nullable=True)
    #
    auditor = Column(String, nullable=True)
    custodian = Column(String, nullable=True)
    guarantor = Column(String, nullable=True)
    beta = Column(Float, nullable=True)
    alpha = Column(Float, nullable=True)
    is_completed = Column(Boolean, nullable=False)
    five_best = Column(Float, nullable=True)
    stock = Column(Float, nullable=True)
    bond = Column(Float, nullable=True)
    other = Column(Float, nullable=True)
    cash = Column(Float, nullable=True)
    deposit = Column(Float, nullable=True)
    fund_unit = Column(Float, nullable=True)
    commodity = Column(Float, nullable=True)
    fund_publisher = Column(BigInteger, nullable=True)
    #
    small_symbol_name = Column(String, nullable=True)
    ins_code = Column(String, nullable=True)
    fund_watch = Column(String, nullable=True)
Base.metadata.create_all(eg)

# 4- store data in table
for values in data_list:
    row = CreateDb(
        reg_no=values.reg_no,
        name=values.name,
        rank_of_12_month=values.rank_of_12_month,
        rank_of_24_month=values.rank_of_24_month,
        rank_of_36_month=values.rank_of_36_month,
        rank_of_48_month=values.rank_of_48_month,
        rank_of_60_month=values.rank_of_60_month,
        rank_last_update=values.rank_last_update,
        fund_type=values.fund_type,
        type_of_invest=values.type_of_invest,
        fund_size=values.fund_size,
        initiation_date=values.initiation_date,
        daily_efficiency=values.daily_efficiency,
        weekly_efficiency=values.weekly_efficiency,
        monthly_efficiency=values.monthly_efficiency,
        quarterly_efficiency=values.quarterly_efficiency,
        six_month_efficiency=values.six_month_efficiency,
        annual_efficiency=values.annual_efficiency,
        statistical_nav=values.statistical_nav,
        efficiency=values.efficiency,
        cancel_nav=values.cancel_nav,
        issue_nav=values.issue_nav,
        dividend_interval_period=values.dividend_interval_period,
        guaranteed_earning_rate=values.guaranteed_earning_rate,
        data=values.data,
        net_asset=values.net_asset,
        estimated_earning_rate=values.estimated_earning_rate,
        invested_units=values.invested_units,
        articles_of_association_link=values.articles_of_association_link,
        prospectus_link=values.prospectus_link,
        website_address=','.join(values.website_address),
        manager=values.manager,
        manager_seo_register_no=values.manager_seo_register_no,
        guarantor_seo_register_no=values.guarantor_seo_register_no,
        auditor=values.auditor,
        custodian=values.custodian,
        guarantor=values.guarantor,
        beta=values.beta,
        alpha=values.alpha,
        is_completed=values.is_completed,
        five_best=values.five_best,
        stock=values.stock,
        bond=values.bond,
        other=values.other,
        cash=values.cash,
        deposit=values.deposit,
        fund_unit=values.fund_unit,
        commodity=values.commodity,
        fund_publisher=values.fund_publisher,
        small_symbol_name=values.small_symbol_name,
        ins_code=values.ins_code,
        fund_watch=values.fund_watch,
    )
    se.add(row)
se.commit()