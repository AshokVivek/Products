# NOT USED ANYMORE !!!
# def check_name_correctness(name):
#     """
#     Check correctness of the name
#     :param: name (to be checked)
#     :return: bool (indicating whether the name is correct or not)
#     """
#     # first check that it should have at least three continuous alphabets somewhere
#     if not re.match('.*[a-zA-Z]{3}.*', name):
#         return False
#     # special character like @ should not be in name
#     # this can come in cases where UPI transactions are picked by mistake here
#     # they get good prediction score, so needs to be skipped by the check below
#     elif '@' in name:
#         return False
#     # this is handled in case transactions are picked in name , and numbers are also present
#     # therefore skip these type of names
#     numbers = sum(c.isdigit() for c in name)
#     if numbers > 10:
#         return False
#     # now check whether its a company name
#     name_start_words = ["M/S", 'MR', "ADVANCED"]
#     temp_name = name.upper().strip()
#     if check_company_name(name):
#         return True
#     for start_word in name_start_words:
#         if temp_name.startswith(start_word):
#             return True
#     # if company name not found, look for a person's name
#     try:
#         # prod always
#         url = BC_ML_UTILS_BASE_API_URL + "bank_connect_ml/check_name"
#         payload = json.dumps({"name": name, "internal_secret": BC_ML_UTILS_INTERNAL_SECRET})
#         response = requests.post(url=url, data=payload, timeout=3).json()
#         confidence = float(response.get("data", {}).get('confidence', 0.0))
#         print(name, confidence)
#         if confidence > 0.23:
#             return True
#     except Exception as e:
#         # print exception
#         print(e)
#         # ignoring errors like SSL and OCR server down
#         return True
#     return False

#todo  Stemming of Company names

# def strip_suffixes(s,suffixes):
#     for suf in suffixes:
#         print(s, suf)
#         if s.endswith(suf):
#             return s.rstrip(suf)
#     return s

# check for company name
def check_company_name(name, country='IN', company_end_keywords_list = []):
    ####################################################################################################################
    ##  This company_end_words list has been moved to bank_connect_fsmlibgeneraldata with tag = 'company_end_words'.   
    ##      We need to maintain this list according to table list here as well.                              
    ####################################################################################################################
    company_end_words = ["PRIVATE", "PVT", "PVT.", "(P)", "LIMITED", "LTD", "LTD.", "LLP", "LLP.",
                         "LIMITED LIABILITY PARTNERSHIP", "INTERNATIONAL", "ENTERPRISES",
                         "SOLUTIONS", "INDUSTRIES", "AGENCIES", "TRADERS", "ELECTRICALS",
                         "STEELS", "CREATIONS", "GRAPHICS", "COMPUTER", "PRINTERS", "SPM",
                         "BOXE", "FACTORY", "CO.", "CO", "MEDICOS", "ENGINEERING WORKS", "PACKAGING", "POWER SYSTEMS",
                         "PAPER PRODUCTS", "CENTRE", "CITY", "SERVICE", "SEWING", "GARMENTS", "TRADER", "ENTERPRISE",
                         "SERVICES", "BOUTIQUE", "MARKET", "CORPORATION", "CREATION", "ENERGY", "LUBRICANTS", "COMPANY",
                         "ELECTRONICS", "KING", "FASHION", "TECHNOLOGIES", "WORK", "BATTERIES", "STORE", "ENGINEERING",
                         "PARTS", "TECHNOLOGY", "EXPERTS", "CONSTRUCTION", "TEXTILES", "SOLUTIONS", "TRADING", "ASSOCIATE",
                         "GLASS", "LOGISTICS", "GROUP", "FORUM", "BAZAAR", "IMPEX", "KNITS", "TECK", "ASSOCIATES", "GROUPS", "CRAFT",
                         "TILES", "CHETS", "STATIONERS", "AGENCY", "SPORT", "TELECOM", "SOCIETY(R)", "SUPPLIYER", "WORLD", "FITNESS",
                         "LABS", "ENGINEERS", "HYDROLIC", "HOUSE", "MOMOS", "LEATHER", "LAUNCHERS", "MOVERS", "STORES", "TEXTILE", "CLASSES",
                         "RESTAURANT", "STATION", "POINT", "JEWELLERS", "LIGHTS", "COLLECTION", "FLOORING", "SOLUTION", "OVERSEAS", "CONTRACTOR",
                         "TECH", "SYSTEMS", "ARTS", "TOOLS", "SPACE", "JUNCTION", "MARKETING", "DESIGNS", "TRADES", 'SAREE', 'HANGERS', 'PAPERS', 'HOTEL', 'BAGS',
                         "JEWEL", "MULTIPLAST", "POLYPLAST", "PRODUCTS", "(BULLION A/C)", "REALTY AOP", "GOLD", "SHINES", "AGRI SCIEN", "PROFESSIONAL", "PLASTICS",
                         "HOSIERY", "STONE", "INFRA",'ACADEMY', 'ACCESORIES', 'ACCESSORY', 'ACCOMMODATION', 'ACCOMODATION', 'ACCOUNTING', 'ACCUMULATORS', 'ACOUSTIC', 
                         'ADVERTISEMENT', 'ADVISOR', 'ADVISORY', 'AEROCARE', 'AFFAIRS', 'AGARBATI', 'AGARBATTHI', 'AGENCEIS', 'AGENCEY', 'AGENCI', 'AGRICARE', 'AGRICHEM', 
                         'AGRICULTURAL', 'AGRICULTURE', 'AGRO', 'AGROCHEMICALS', 'AGROS', 'AJENCY', 'AMBULANCE', 'APPARELLS', 'APPARELSS', 'APPEALING', 'APPEARLS', 'APPLIANCE', 
                         'AQUATIC', 'ARCADE', 'ARCHITECT', 'ARCHITECTS', 'ARCHITECTURAL', 'ARENA', 'ARTIFICIAL', 'ARTIFICIALS', 'ASOCIATES', 'ASPIRANTS', 'ASSETS', 'ASSOICATES', 
                         'AUTHORITY', 'AUTOCRAFTS', 'AUTOFURNISH', 'AUTOHUB', 'AUTOMATIONS', 'AUTOMOBAILE', 'AUTOMOBILES', 'AUTOMOVERS', 'AUTOPARTS', 'AUTOSPARES', 'AUTOTECH', 
                         'AUTOWHEELS', 'AYURVEDIK', 'AYURVIDIC', 'BAKERIE', 'BAKES', 'BAKING', 'BALLOONS', 'BANGALES', 'BANGLES', 'BANNERS', 'BBQ', 'BEARINGS', 'BEAUTIPARLOUR', 
                         'BEAUTIQUE', 'BEDS', 'BEEF', 'BEKRY', 'BEVARAGES', 'BHAJI', 'BHAJIYA', 'BHANDAAR', 'BHANDARIYA', 'BHANDHAR', 'BHOJANALAY', 'BHOJNALAYA', 'BICKERS', 'BIKES', 
                         'BINDERS', 'BINDIS', 'BIOCHEMICALS', 'BIOENERGIES', 'BIOMEDICAL', 'BIOTECHNIQS', 'BIRIYANI', 'BIRYANIWALA', 'BISTRO', 'BITES', 'BLEACHERS', 'BLENDERS', 
                         'BOARING', 'BOILER', 'BOOKING', 'BOOTS', 'BOREWELL', 'BOREWELLS', 'BOTIQUE', 'BOTTELS', 'BOTTLE', 'BOTTLES', 'BOXES', 'BRANDING', 'BRANDINGS', 'BRICK', 
                         'BROILER', 'BROILERS', 'BROKER', 'BROKERS', 'BROOM', 'BROOMS', 'BRUSH', 'BUCKET', 'BUILDER', 'BUREAU', 'BURGER', 'BURNING', 'BUSINESSES', 'BUTIQ', 'CABINS', 
                         'CABS', 'CAFFE', 'CAKES', 'CAMERA', 'CAMPING', 'CANDLES', 'CAPITALS', 'CARBON', 'CARDS', 'CAREERS', 'CARETAKER', 'CARNIVAL', 'CARPET', 'CARRIAGE', 'CARS', 
                         'CARSELLING', 'CARTEL', 'CARTINGS', 'CARTON', 'CARWASHING', 'CASHEW', 'CASUALS', 'CATALYST', 'CATERER', 'CATERINGS', 'CATERS', 'CATRING', 'CATTERING', 
                         'CATTERS', 'CATTLEFEED', 'CATTLEFEEDS', 'CELEBRATIONS', 'CEMENTS', 'CENTERING', 'CHANNELS', 'CHARGING', 'CHEESE', 'CHEMICALS', 'CHEMISTS', 'CHICKENS', 
                         'CHICKS', 'CHIKEN', 'CHIPS', 'CHIPSETS', 'CHOCOLATE', 'CHOCOLATES', 'CHOUPATI', 'CINEMAS', 'CLEANER', 'CLEANERS', 'CLOTHES', 'CLOTHIING', 'CLOTHINGS', 
                         'CLOTHS', 'CLOTHSHOP', 'COLECTION', 'COMAPNY', 'COMFORT', 'COMFORTS', 'COMMERCIALS', 'COMMODITIES', 'COMMUNCATION', 'COMPNAY', 'CONSTRUCTON', 'CONSTRUSTION', 
                         'CONSTTRUCTION', 'CONSTURCTION', 'CONSULT', 'CONSULTAN', 'CONTRACTORS', 'CONTRATOR', 'CONTRUCTION', 'CONTRUCTIONS', 'CONVENTION', 'CONVENTIONS', 'CONVEYORS', 
                         'COOKWARE', 'COOLDRINKS', 'COOLING', 'COSMETIC', 'COSTMATICS', 'COSTUME', 'COSTUMES', 'COTTONS', 'COURIER', 'COURIERS', 'COVERS', 'CRACKERY', 'CRAVINGS', 
                         'CREAM', 'CREAMS', 'CREATORS', 'CREDITS', 'CROCKERY', 'CRUSHER', 'CRUSHERS', 'CRUSHING', 'CRYSTALS', 'CULTURE', 'CUTLERY', 'CUTPIECE', 'CUTPIECES', 'CYCLES', 
                         'DABELI', 'DEALERS', 'DECORATIONS', 'DECORATIVE', 'DECORATOR', 'DECORS', 'DELITES', 'DESIGNING', 'DESSERTS', 'DESTINATION', 'DESTINATIONS', 'DETERGENT', 
                         'DETERGENTS', 'DEVLOPERS', 'DIARIES', 'DISTRIBUTER', 'DISTRIBUTERS', 'DISTRIBUTION', 'DISTRIBUTORS', 'DOORS', 'DORMITORY', 'DREESES', 'DRESES', 'DRESSESS', 
                         'DRILLING', 'DRILLS', 'DRONES', 'DROPSHIP', 'DRUG', 'DRUGS', 'DRYCLEANER', 'DRYCLEANERS', 'DRYCLEANING', 'DRYER', 'ELCTRICALS', 'ELCTRONICS', 'ELECRICALS', 
                         'ELECRONICES', 'ELECTONIC', 'ELECTONICS', 'ELECTRIAL', 'ELECTRIC', 'ELECTRICAL', 'EMPOWERING', 'ENETERPRISES', 'ENGGINEERING', 'ENGGINERING', 'ENGIEERING', 
                         'ENGINERRING', 'ENGINIRING', 'ENGINNERING', 'ENTARPRISES', 'ENTEERPRISES', 'ENTEPRISE', 'ENTEPRISES', 'ENTERPEISES', 'ENTERPERISES', 'ENTERPIRSES', 
                         'ENTERPISE', 'ENTERPISES', 'ENTERPRICE', 'ENTERPRIES', 'ENTERPRIESES', 'ENTERPRIS', 'ENTERPRISESS', 'ENTERPRISIS', 'ENTERPRSE', 'ENTERRISE', 'ENTERRPISES', 
                         'ENTERRPRISES', 'ENTERTAINMENTS', 'ENTREPRISES', 'ENTRPRISES', 'ESSAR', 'ESSENTIAL', 'ESSENTIALS', 'ESTABLISHMENT', 'ESTABLISHMENTS', 'ESTEEM', 'EXPLOSIVES', 
                         'EXTERIORS', 'FABRICATERS', 'FABRICATIONS', 'FABRICATOR', 'FABRICATORS', 'FACILITIES', 'FAISHION', 'FALOODA', 'FASCILITIES', 'FASHON', 'FASTENERS', 'FASTFOOD', 
                         'FERTILISERS', 'FERTILITY', 'FERTLIZERS', 'FERTILIZER', 'FILES', 'FINANCES', 'FINANCIALS', 'FINISHERS', 'FIXING', 'FLAGS', 'FLORALS', 'FOODIE', 'FOODIES', 
                         'FOODWORKS', 'FOOTWARE', 'FORMALS', 'FOUNDERS', 'FRAMERS', 'FURNACE', 'FURNACES', 'FURNISHERS', 'FURNISHING', 'FURNISHINGS', 'GADGETS', 'GAMES', 'GARDENS', 
                         'GARMENAT', 'GARMENT', 'GLOBAL', 'GOODIES', 'HAIRS', 'HANDICRAFT', 'HANDICRAFTS', 'HARDWARE', 'HARDWERE', 'HARVESTER', 'HARWARE', 'HEATHCARE', 'HEATWORKS', 
                         'HOLDINGS', 'HOLIDAYS', 'INDISTRIES', 'INKJET', 'INNOVATION', 'INNOVATIVES', 'INSITUTE', 'INVESTOR', 'JAGGERY', 'JEWELS', 'KACHORI', 'KEEPERS', 'KIDSWEAR', 
                         'KIRAANA', 'KIRANAA', 'KNITTERS', 'KNITTING', 'KNITTINGS', 'KNITWEAR', 'KURTI', 'KURTIS', 'LAPTOPS', 'LAUNDRY', 'LAWNS', 'LAYOUTS', 'LEGGING', 'LEGGINGS', 
                         'LIGHTHOUSE', 'LIMTED', 'LOCKS', 'LODGE', 'LODGING', 'LOGISTIKS', 'LOGISTIX', 'LUBRRICANTS', 'MAART', 'MANEJMENT', 'MANGEMENT', 'MANUFACTUR', 'MANUFACTURES', 
                         'MARBELS', 'MARKETERS', 'MARKETINGS', 'MARKETTING', 'MART', 'MATTRESS', 'MATTRESSES', 'MEAT', 'MEATS', 'MECHANICAL', 'MECHANICALS', 'MEDICAL', 'MEDICALS', 
                         'MEDICINES', 'MEDICOES', 'METAL', 'MINING', 'MOBILE', 'MOTORS', 'MOULDERS', 'MOULDING', 'MOVIES', 'MULTIVENTURES', 'MUSICS', 'NIGHTWEAR', 'NOODLES', 
                         'NOVALTIES', 'NOVELTIES', 'NURESRY', 'NUTRITION', 'NUTRITIONS', 'OILS', 'OPERATIONS', 'OPERATORS', 'OPTICALS', 'OPTICS', 'ORGANISER', 'ORGNIZATION',
                           'ORIGINALS', 'ORIGINS', 'ORNAMENT', 'ORNAMENTAL', 'ORNAMENTS', 'ORTHOCARE', 'OUTDOORS', 'PACKAGER', 'PACKAGINGS', 'PACKINGS', 'PAKAGING', 'PARTNERS', 
                           'PAVBHAJI', 'PERFORATORS', 'PERFUMERY', 'PERFUMES', 'PESTICEDES', 'PESTICIDS', 'PESTICIEDS', 'PESTISIDES', 'PETROCHEMICALS', 'PETROLIUM', 'PETROLUEM', 
                           'PHARAMACY', 'PHARMACEUTICALES', 'PHOTOGRAPHY', 'PICTURES', 'PIONEERS', 'PIZZAS', 'PLATERS', 'PORTER', 'POTATOES', 'PROCESSOR', 'PROCESSORS', 'PRODUCER', 
                           'PRODUCTION', 'PRODUCTIONS', 'PROFICIENCY', 'PROJECTS', 'PROMOTORS', 'PROOFING', 'PROPERITER', 'PROPERITOR', 'PROPERTIES', 'PROPERTY', 'PROPRITER', 'PROTECTIONS', 
                           'PROTEIN', 'PROTEINS', 'PROTIENS', 'PUBLICITY', 'PUBLISHERS', 'PUBLISHING', 'PVTLTD', 'RADIATOR', 'RADIATORS', 'RAILING', 'RASOYI', 'RECRUITER', 'RECRUITERS', 
                           'RECRUITMENT', 'RECYCLERS', 'REFINERIES', 'REFINERY', 'REFINISHINGS', 'REFRACTORIES', 'REFRACTORY', 'REFREGERATION', 'REFRESH', 'REFRESHMENT', 'REFRIGERATIONS', 
                           'REFRIGERATOR', 'REFRIGERATORS', 'REGIMENT', 'REGIONAL', 'REGISTRATION', 'REPAIR', 'REPAIRING', 'REPAIRS', 'REPARING', 'REPEARING', 'REPROCESSING', 'RESIDENCES', 
                           'RESIDENTIAL', 'RESORT', 'RESORTS', 'RESTAURANTS', 'RESTAURENT', 'RESTO', 'RESTURANT', 'RETAILER', 'SALES', 'SANATARY', 'SANITARIES', 'SANITATIONS', 'SCRAPING', 
                           'SCREENS', 'SEAFOODS', 'SECTORS', 'SECURES', 'SECURITY', 'SEEDS', 'SELECTIONS', 'SEMICONDUCTORS', 'SENETARY', 'SERIVCES', 'SERVICING', 'SERVICS', 'SERVISES',
                             'SETTERS', 'SHIRTING', 'SHIRTINGS', 'SHOWROOM', 'SILKS', 'SINTHETICS', 'SKILLS', 'SOLLUTION', 'SOLUTIOINS', 'SONOGRAPHY', 'SOULUTIONS', 'SPECIALIST', 
                             'SPORTS', 'SPORTSWEAR', 'STATIONER', 'STATIONERIES', 'STATIONS', 'STAYS', 'STICHERS', 'STICKERS', 'STITCHERS', 'STOCKS', 'STOERS', 'STONES', 'STORIES', 
                             'STREETS', 'SUPPLY', 'SUPPORTS', 'SWITCH', 'SWITCHES', 'SYNDICATE', 'SYNECTICS', 'SYNERGISTIC', 'SYNERGY', 'SYNTHETIC', 'SYNTHETICS', 'SYSTEMATIC', 
                             'SYSTSEM', 'TABACCO', 'TABLE', 'TABLEWARE', 'TAEKWONDO', 'TAILORING', 'TANDOOR', 'TANDOORI', 'TAPS', 'TASTE', 'TASTY', 'TATTOO', 'TAX', 'TECHLAB', 
                             'TECHNIQUE', 'TECHNIQUES', 'TECHNOLOGYS', 'TECHPARK', 'TELECOME', 'TELECOMMUNICATION', 'TELECOMMUNICATIONS', 'TELEFILMS', 'TELEPHONE', 'TELESERVICES', 
                             'TENDER', 'TERMINAL', 'TESTING', 'TEXTILESS', 'TEXTURE', 'TEXTURES', 'THAAI', 'THERAPUTICS', 'THERAPY', 'THERMAL', 'TIMBERS', 'TOBACCO', 'TOBACO', 
                             'TOWERS', 'TOY', 'TRACKING', 'TRADDERS', 'TRADEERSS', 'TRADERES', 'TRADERSS', 'TRAILER', 'TRAILERS', 'TRAILOR', 'TRAILORS', 'TRANSFORMATION', 
                             'TRANSMISSIONS', 'TRANSPLANT', 'TRANSPORT', 'TRANSPORTING', 'TRANSPORTS', 'TRAVELLER', 'TRAVELLERS', 'TRAVELLING', 'TRAVLES', 'TREASURE', 'TREASURES', 
                             'TREASURY', 'TREAT', 'TREATS', 'TREND', 'TRENDING', 'TRENDS', 'TRICYCLE', 'TRICYCLES', 'TRIPS', 'TROLLEY', 'TRUCKS', 'TUTORIAL', 'TUTORIALS', 'TVS', 
                             'TWISTING', 'TYERS', 'TYPES', 'TYRES', 'UDAAN', 'ULTRASOUND', 'UNIFORM', 'UNIFORMS', 'UNITE', 'UNLIMITED', 'VACATION', 'VACATIONS', 'VACCINES', 'VACCUM',
                               'VADAPAV', 'VALLEY', 'VARIETIES', 'VARITEY', 'VEG', 'VEGETARIAN', 'VEGETEBLES', 'VEGGIES', 'VEGITABLES', 'VEHICLE', 'VENTURE', 'VERITY', 'VIBRANT', 
                               'VIBRATIONS', 'VIDEO', 'VIDEOS', 'VISUAL', 'VISUALS', 'WADAPAAV', 'WAFFLES', 'WALLPAPER', 'WALLPAPERS', 'WARDROBE', 'WARDROBES', 'WEARS', 'WEAVERS',
                                 'WEDDINGS', 'WELDERS', 'WHEELS', 'WHEELZ', 'WINDOW', 'WINDOWS', 'WINES', 'WIRES', 'WIRESTRIPS', 'WORKSHOP', 'CHEM', 'PRINTS',
                                 'COMPUTER', 'COMPUTERS', 'UDYOG', 'BROTHERS', 'BROTHER', 'MEDIA', 'BEEJ', 'FURNITURE', 'MILL', 'MACHINES', 'FOOD', 'BAKERY', 'COMMUNICATION',
                         'SOFA', 'MACHINE', 'PLASTIC', 'DESIGN', 'MOBILES', 'ART', 'PLASTICS', 'MOBILE', 'VENTURES', 'MANUFACTURING', 'FASHIONS', 'SONS', 'VENTURE', 'FASHION',
                         'MODULA', 'PHARMACEUTICALS', 'AIRCON', 'RESTAUR', 'FOODS', 'MAKER', "TUBES", "PIPE", "PIPES", "TUBE", "CATERERS", "COATING", "CUISINE", "DECORATORS",
                         "DEPARTMENT", "DIGITAL", "DRESSES", "ELECTRONIC", "EVENTS", "FABRICATION", "FOOTWEAR", "FOOTWEARS", "FRAMES", "GENERAL", "GENERALS", "GRANITE",
                         "MANGES", "NEEDS", "OWNERS", "PACKERS", "PARADISE", "PLYWOOD", "WEAR", "WORKS"]
    ####################################################################################################################
    ##  This company_end_words list has been moved to bank_connect_fsmlibgeneraldata with tag = 'company_end_words'.
    ##      We need to maintain this list according to table list here as well.                
    ####################################################################################################################
    
    company_end_words_id = ["PT", "PTE", "CV"]

    if len(company_end_keywords_list)==0:
        if country == 'ID':
            company_end_keywords_list = company_end_words_id
        else:
            company_end_keywords_list = company_end_words

    # temp_name = strip_suffixes(name.upper().strip(), ['IES', 'ES', 'S'])
    temp_name = name.upper().strip()
    for end_word in company_end_keywords_list:
        if temp_name.endswith(end_word):
            return True, end_word
    
    split_name = temp_name.split()
    if country == 'ID':
        for word in company_end_keywords_list:
            if word in split_name:
                return True, word
    
    return False, ''
