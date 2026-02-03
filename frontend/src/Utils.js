export const companies = {
    "Adobe Inc." : "ADBE",
    "ADP" : "ADP",
    "AMD" : "AMD",
    "Analog Devices" : "ADI" ,
    "Ansys" : "ANSS",
    "Apple Inc." : "AAPL",
    "Applied Materials" : "AMAT",
    "ASML Holding" : "ASML",
    "Atlassian" : "TEAM",
    "Autodesk" : "ADSK",
    "Broadcom Inc." : "AVGO",
    "Cadence Design Systems" : "CDNS", 
    "Cisco" : "CSCO",
    "Cognizant" : "CTSH",
    "CrowdStrike" : "CRWD",
    "Datadog" : "DDOG",
    "DocuSign" : "DOCU",
    "Fiserv" : "FISV",
    "Fortinet" : "FTNT",
    "Intel" : "INTC",
    "Intuit" : "INTU",
    "KLA Corporation" : "KLAC", 
    "Lam Research" : "LRCX",
    "Marvell Technology" : "MRVL",
    "Microchip Technology" : "MCHP",
    "Micron Technology" : "MU",
    "Microsoft" : "MSFT",
    "Nvidia" : "NVDA",
    "NXP" : "NXPI",
    "Okta, Inc." : "OKTA",
    "Palo Alto Networks" : "PANW", 
    "Paychex" : "PAYX",
    "PayPal" : "PYPL",
    "Qualcomm" : "QCOM",
    "Skyworks Solutions" : "SWKS", 
    "Splunk" : "SPLK",
    "Synopsys" : "SNPS",
    "Texas Instruments" : "TXN",
    "Verisign" : "VRSN",
    "Workday, Inc." : "WDAY",
    "Zoom Video Communications" : "ZM", 
    "Zscaler" : "ZS",
    "Activision Blizzard" : "ATVI", 
    "Alphabet Inc. (Class A)" : "GOOGL",
    "Alphabet Inc. (Class C)" : "GOOG",
    "Baidu" : "BIDU",
    "Charter Communications" : "CHTR",
    "Comcast" : "CMCSA",
    "Electronic Arts" : "EA",
    "Match Group" : "MTCH",
    "Meta Platforms" : "META",
    "NetEase" : "NTES",
    "Netflix" : "NFLX",
    "Sirius XM" : "SIRI"
}

export const industry = {
	APPLICATION_SOFTWARE: "Application Software",
    DATA_PROCESSING_OUTSOURCED_SERVICES: "Data Processing & Outsourced Services",
    SEMICONDUCTORS: "Semiconductors",
    TECHNOLOGY_HARDWARE: "Technology Hardware, Storage & Peripherals",
    SEMICONDUCTOR_EQUIPMENT: "Semiconductor Equipment",
    COMMUNICATIONS_EQUIPMENT: "Communications Equipment",
    IT_CONSULTING: "IT Consulting & Other Services",
    SYSTEMS_SOFTWARE: "Systems Software",
    INTERNET_SERVICES: "Internet Services & Infrastructure",
    INTERACTICE_HOME_ENTERTAINMENT: "Interactive Home Entertainment",
    INTERACTIVR_MEDIA_SERVICES: "Interactive Media & Services",
    CABLE_SATELLITE: "Cable & Satellite",
	MOVIES_ENTERTAINMENTS: "Movies & Entertainments",
	BORADCASTING: "Broadcasting"
}

export function getIndustryByCompany(company){
    switch(company){
      case "ADBE":case "ANSS":case "TEAM":case "ADSK":case "CDNS":case "CRWD":case "DDOG":case "DOCU":case "INTU":case "MRVL":case "OKTA":case "PANW":case "SPLK":case "SNPS":case "WDAY":case "ZM":case "ZS":
        return industry.APPLICATION_SOFTWARE;
      case "ADP":case "FISV":case "PAYX":case "PYPL":
        return industry.DATA_PROCESSING_OUTSOURCED_SERVICES
      case "AMD":case "ADI":case "AVGO":case "INTC":case "MCHP":case "MU":case "NVDA":case "NXPI":case "QCOM":case "SWKS":case "TXN":
        return industry.SEMICONDUCTORS
      case "AAPL":
        return industry.TECHNOLOGY_HARDWARE
      case "AMAT":case "ASML":case "KLAC":case "LRCX":
        return industry.SEMICONDUCTOR_EQUIPMENT
      case "CSCO":
        return industry.COMMUNICATIONS_EQUIPMENT
      case "CTSH":
        return industry.IT_CONSULTING
      case "FTNT":case "MSFT":
        return industry.SYSTEMS_SOFTWARE
      case "VRSN":
        return industry.INTERNET_SERVICES
      case "ATVI":case "EA":case "NTES":
        return industry.INTERACTICE_HOME_ENTERTAINMENT
      case "GOOGL":case "GOOG":case "BIDU":case "MTCH":case "META":
        return industry.INTERACTIVR_MEDIA_SERVICES
      case "CHTR":case "CMCSA":
        return industry.CABLE_SATELLITE
      case "NFLX":
        return industry.MOVIES_ENTERTAINMENTS
      case "SIRI":
        return industry.BORADCASTING
      default:
        return "Not found"
    }
  }