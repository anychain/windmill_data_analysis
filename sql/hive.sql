SET fs.default.name;


CREATE TABLE IF NOT EXISTS default.TABLE_R(`time` String, ID int, K1 int, K2 int, K3 int, K4 int, K5 int, K6 int, 
    K7 int, K8 int, K9 int, K10 int, K11 int, K12 int, K13 int, K14 int, K15 int, K16 int, 
    K17 int, K18 int, K19 int, K20 int, K21 int, K22 int, K23 int, K24 int, K25 int, K26 int, 
    K27 int, K28 int, K29 int, K30 int, K31 int, K32 int, K33 int, K34 int, K35 int, K36 int, 
    K37 int, K38 int, K39 int, K40 int, K41 int, K42 int, K43 int, K44 int, K45 int, K46 int, 
    K47 int, K48 int, K49 int, K50 int, K51 int, K52 int, K53 int, K54 int, K55 int, K56 int, 
    K57 int, K58 int, K59 int, K60 int, K61 int, K62 int, K63 int, K64 int, K65 int, K66 int, 
    K67 int, K68 int, K69 int, K70 int, K71 int, K72 int, K73 int, K74 int, K75 int, K76 int, 
    K77 int, K78 int, K79 int, K80 int, K81 int, K82 int, K83 int, K84 int, K85 int, K86 int, 
    K87 int, K88 int, K89 int, K90 int, K91 int, K92 int, K93 int, K94 int, K95 int, K96 int, 
    K97 int, K98 int, K99 int, K100 int, K101 int, K102 int, K103 int, K104 int, K105 int, 
    K106 int, K107 int, K108 int, K109 int, K110 int, K111 int, K112 int, K113 int, K114 int, 
    K115 int, K116 int, K117 int, K118 int, K119 int, K120 int, K121 int, K122 int, K123 int, 
    K124 int, K125 int, K126 int, K127 int, K128 int, K129 int, K130 int, K131 int, K132 int, 
    K133 int, K134 int, K135 int, K136 int, K137 int, K138 int, K139 int, K140 int, K141 int, 
    K142 int, K143 int, K144 int, K145 int, K146 int, K147 int, K148 int, K149 int, K150 int, 
    K151 int, K152 int, K153 int, K154 int, K155 int, K156 int, K157 int, K158 int, K159 int, 
    K160 int, K161 int, K162 int, K163 int, K164 int, K165 int, K166 int, K167 int, K168 int, 
    K169 int, K170 int, K171 int, K172 int, K173 int, K174 int, K175 int, K176 int, K177 int, 
    K178 int, K179 int, K180 int, K181 int, K182 int, K183 int, K184 int, K185 int, K186 int, 
    K187 int, K188 int, K189 int, K190 int, K191 int, K192 int, K193 int, K194 int, K195 int, 
    K196 int, K197 int, K198 int, K199 int, K200 int, K201 int, K202 int, K203 int, K204 int, 
    K205 int, K206 int, K207 int, K208 int, K209 int, K210 int, K211 int, K212 int, K213 int, 
    K214 int, K215 int, K216 int, K217 int, K218 int, K219 int, K220 int, K221 int, K222 int, 
    K223 int, K224 int, K225 int, K226 int, K227 int, K228 int, K229 int, K230 int, K231 int, 
    K232 int, K233 int, K234 int, K235 int, K236 int, K237 int, K238 int, K239 int, K240 int, 
    K241 int, K242 int, K243 int, K244 int, K245 int, K246 int, K247 int, K248 int, K249 int, 
    K250 int, K251 int, K252 int, K253 int, K254 int, K255 int, K256 int, K257 int, K258 int, 
    K259 int, K260 int, K261 int, K262 int, K263 int, K264 int, K265 int, K266 int, K267 int, 
    K268 int, K269 int, K270 int, K271 int, K272 int, K273 int, K274 int, K275 int, K276 int, 
    K277 int, K278 int, K279 int, K280 int, K281 int, K282 int, K283 int, K284 int, K285 int, 
    K286 int, K287 int, K288 int, K289 int, K290 int, K291 int, K292 int, K293 int, K294 int, 
    K295 int, K296 int, K297 int, K298 int, K299 int, K300 int, K301 int, K302 int, K303 int, 
    K304 int, K305 int, K306 int, K307 int, K308 int, K309 int, K310 int, K311 int, K312 int, 
    K313 int, K314 int, K315 int, K316 int, K317 int, K318 int, K319 int, K320 int, K321 int, 
    K322 int, K323 int, K324 int, K325 int, K326 int, K327 int, K328 int, K329 int, K330 int, 
    K331 int, K332 int, K333 int, K334 int, K335 int, K336 int, K337 int, K338 int, K339 int, 
    K340 int, K341 int, K342 int, K343 int, K344 int, K345 int, K346 int, K347 int, K348 int, 
    K349 int, K350 int,
    TAG1 float, TAG2 float, TAG3 float, TAG4 float, TAG5 float, TAG6 float, 
    TAG7 float, TAG8 float, TAG9 float, TAG10 float, TAG11 float, TAG12 float, TAG13 float,
    TAG14 float, TAG15 float, TAG16 float, TAG17 float, TAG18 float, TAG19 float, TAG20 float,
    TAG21 float, TAG22 float, TAG23 float, TAG24 float, TAG25 float, TAG26 float, TAG27 float,
    TAG28 float, TAG29 float, TAG30 float, TAG31 float, TAG32 float, TAG33 float, TAG34 float,
    TAG35 float, TAG36 float, TAG37 float, TAG38 float, TAG39 float, TAG40 float, TAG41 float,
    TAG42 float, TAG43 float, TAG44 float, TAG45 float, TAG46 float, TAG47 float, TAG48 float,
    TAG49 float, TAG50 float, TAG51 float, TAG52 float, TAG53 float, TAG54 float, TAG55 float,
    TAG56 float, TAG57 float, TAG58 float, TAG59 float, TAG60 float, TAG61 float, TAG62 float,
    TAG63 float, TAG64 float, TAG65 float, TAG66 float, TAG67 float, TAG68 float, TAG69 float,
    TAG70 float, TAG71 float, TAG72 float, TAG73 float, TAG74 float, TAG75 float, TAG76 float, 
    TAG77 float, TAG78 float, TAG79 float, TAG80 float, TAG81 float, TAG82 float, TAG83 float,
    TAG84 float, TAG85 float, TAG86 float, TAG87 float, TAG88 float, TAG89 float, TAG90 float,
    TAG91 float, TAG92 float, TAG93 float, TAG94 float, TAG95 float, TAG96 float, TAG97 float,
    TAG98 float, TAG99 float, TAG100 float, TAG101 float, TAG102 float, TAG103 float, TAG104 float,
    TAG105 float, TAG106 float, TAG107 float, TAG108 float, TAG109 float, TAG110 float, TAG111 float,
    TAG112 float, TAG113 float, TAG114 float, TAG115 float, TAG116 float, TAG117 float, TAG118 float,
    TAG119 float, TAG120 float, TAG121 float, TAG122 float, TAG123 float, TAG124 float, TAG125 float,
    TAG126 float, TAG127 float, TAG128 float, TAG129 float, TAG130 float, TAG131 float, TAG132 float, 
    TAG133 float, TAG134 float, TAG135 float, TAG136 float, TAG137 float, TAG138 float, TAG139 float,
    TAG140 float, TAG141 float, TAG142 float, TAG143 float, TAG144 float, TAG145 float, TAG146 float,
    TAG147 float, TAG148 float, TAG149 float, TAG150 float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' LINES TERMINATED BY '\n';
LOAD DATA INPATH 'hdfs://192.168.0.206:8020/poc_test/*.csv' into table default.poc_csv;

CREATE TABLE default.TABLE_R_parquet like default.TABLE_R
STORED AS PARQUET;
set COMPRESSION_CODEC=none;

insert overwrite table default.TABLE_R_parquet select * from default.TABLE_R_ORC;

CREATE TABLE default.TABLE_R_ORC like default.TABLE_R STORED AS ORC;

insert overwrite table default.TABLE_R_ORC select * from default.TABLE_R;


