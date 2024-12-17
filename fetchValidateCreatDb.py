import requests
from pydantic import BaseModel
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
    regNo: int
    name: str
    rank0f12Month: Optional[int] = None
    rank0f24Month: Optional[int] = None
    rank0f36Month: Optional[int] = None
    rank0f48Month: Optional[int] = None
    rank0f60Month: Optional[int] = None
    rankLastUpdate: str
    fundType: int
    typeOfInvest: str
    fundSize: Optional[int] = None
    initiationDate: str
    dailyEfficiency: Optional[float] = None
    weeklyEfficiency: Optional[float] = None
    monthlyEfficiency: Optional[float] = None
    quarterlyEfficiency: Optional[float] = None
    sixMonthEfficiency: Optional[float] = None
    annualEfficiency: Optional[float] = None
    statisticalNav: Optional[float] = None
    efficiency: Optional[float] = None
    cancelNav: Optional[float] = None
    issueNav: Optional[float] = None
    dividendIntervalPeriod: Optional[int] = None
    guaranteedEarningRate: Optional[float] = None
    data: Optional[str] = None
    netAsset: Optional[int] = None
    estimatedEarningRate: Optional[float] = None
    investedUnits: Optional[int] = None
    articlesOfAssociationLink: Optional[str] = None
    prosoectusLink: Optional[str] = None
    websiteAddress: list
    manager: str
    managerSeoRegisterNo: Optional[int] = None
    guarantorSeoRegisterNo: Optional[int] = None
    auditor: str
    custodian: str
    guarantor: str
    beta: Optional[float] = None
    alpha: Optional[float] = None
    isCompleted: bool
    fiveBest: Optional[float] = None
    stock: Optional[float] = None
    bond: Optional[float] = None
    other: Optional[float] = None
    cash: Optional[float] = None
    deposit: Optional[float] = None
    fundUnit: Optional[float] = None
    commodity: Optional[float] = None
    fundPublisher: int
    smallSymbolName: Optional[str] = None
    insCode: Optional[str] = None
    fundWatch: Optional[str] = None

data_list = []
for i in range(0,len(data['items'])):
    data_list.append(DataV.model_validate(data['items'][i]))


# 3- create table
eg = create_engine('postgresql://postgres:09101489397@localhost:5432/postgres')
Base = declarative_base()
Se = sessionmaker(bind=eg)
se = Se()

class CreateDb(Base):
    __tablename__ = 'dataBors'
    regno = Column(BigInteger, primary_key=True)
    #
    name = Column(String, nullable=False)
    rankof12month = Column(Float, nullable=True)
    rankof24month = Column(Float, nullable=True)
    rankof36month = Column(Float, nullable=True)
    rankof48month = Column(Float, nullable=True)
    rankof60month = Column(Float, nullable=True)
    ranklastupdate = Column(DateTime, nullable=True)
    fundtype = Column(Integer, nullable=False)
    typeofinvest = Column(String, nullable=False)
    fundsize = Column(BigInteger, nullable=True)
    initiationdate = Column(DateTime, nullable=False)
    dailyefficiency = Column(Float, nullable=True)
    weeklyefficiency = Column(Float, nullable=True)
    monthlyefficiency = Column(Float, nullable=True)
    quarterlyefficiency = Column(Float, nullable=True)
    sixmonthefficiency = Column(Float, nullable=True)
    annualefficiency = Column(Float, nullable=True)
    statisticalnav = Column(Float, nullable=True)
    efficiency = Column(Float, nullable=True)
    cancelnav = Column(Float, nullable=True)
    issuenav = Column(Float, nullable=True)
    dividendintervalperiod = Column(BigInteger, nullable=True)
    #
    guaranteedearningrate = Column(Float, nullable=True)
    data = Column(String, nullable=True)
    netasset = Column(BigInteger, nullable=True)
    estimatedearningrate = Column(Float, nullable=True)
    investedunits = Column(BigInteger, nullable=True)
    articlesofassociationlink = Column(String, nullable=True)
    prosoectuslink = Column(String, nullable=True)
    websiteaddress = Column(String, nullable=True)
    manager = Column(String, nullable=False)
    managerseoregisterno = Column(BigInteger, nullable=True)
    #
    guarantorseoregisterno = Column(BigInteger, nullable=True)
    #
    auditor = Column(String, nullable=True)
    custodian = Column(String, nullable=True)
    guarantor = Column(String, nullable=True)
    beta = Column(Float, nullable=True)
    alpha = Column(Float, nullable=True)
    iscompleted = Column(Boolean, nullable=False)
    fivebest = Column(Float, nullable=True)
    stock = Column(Float, nullable=True)
    bond = Column(Float, nullable=True)
    other = Column(Float, nullable=True)
    cash = Column(Float, nullable=True)
    deposit = Column(Float, nullable=True)
    fundunit = Column(Float, nullable=True)
    commodity = Column(Float, nullable=True)
    fundpublisher = Column(BigInteger, nullable=True)
    #
    smallsymbolname = Column(String, nullable=True)
    inscode = Column(String, nullable=True)
    fundwatch = Column(String, nullable=True)

Base.metadata.create_all(eg)

# 4- store data in table
for values in data_list:
    row = CreateDb(
        regno=values.regNo,
        name=values.name,
        rankof12month=values.rank0f12Month,
        rankof24month=values.rank0f24Month,
        rankof36month=values.rank0f36Month,
        rankof48month=values.rank0f48Month,
        rankof60month=values.rank0f60Month,
        ranklastupdate=values.rankLastUpdate,
        fundtype=values.fundType,
        typeofinvest=values.typeOfInvest,
        fundsize=values.fundSize,
        initiationdate=values.initiationDate,
        dailyefficiency=values.dailyEfficiency,
        weeklyefficiency=values.weeklyEfficiency,
        monthlyefficiency=values.monthlyEfficiency,
        quarterlyefficiency=values.quarterlyEfficiency,
        sixmonthefficiency=values.sixMonthEfficiency,
        annualefficiency=values.annualEfficiency,
        statisticalnav=values.statisticalNav,
        efficiency=values.efficiency,
        cancelnav=values.cancelNav,
        issuenav=values.issueNav,
        dividendintervalperiod=values.dividendIntervalPeriod,
        guaranteedearningrate=values.guaranteedEarningRate,
        data=values.data,
        netasset=values.netAsset,
        estimatedearningrate=values.estimatedEarningRate,
        investedunits=values.investedUnits,
        articlesofassociationlink=values.articlesOfAssociationLink,
        prosoectuslink=values.prosoectusLink,
        websiteaddress=','.join(values.websiteAddress),
        manager=values.manager,
        managerseoregisterno=values.managerSeoRegisterNo,
        guarantorseoregisterno=values.guarantorSeoRegisterNo,
        auditor=values.auditor,
        custodian=values.custodian,
        guarantor=values.guarantor,
        beta=values.beta,
        alpha=values.alpha,
        iscompleted=values.isCompleted,
        fivebest=values.fiveBest,
        stock=values.stock,
        bond=values.bond,
        other=values.other,
        cash=values.cash,
        deposit=values.deposit,
        fundunit=values.fundUnit,
        commodity=values.commodity,
        fundpublisher=values.fundPublisher,
        smallsymbolname=values.smallSymbolName,
        inscode=values.insCode,
        fundwatch=values.fundWatch,
    )
    se.add(row)
se.commit()