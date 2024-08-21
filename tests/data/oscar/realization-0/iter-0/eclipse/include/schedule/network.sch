NETWORK
  100 100 /

GRUPTREE
  'WIST'       'FIELD'       /
  'WIST_OP'    'WIST'        /
  'WIST_WI'    'WIST'        /
  'WIST_GI'    'WIST'        /
  'WIST_CO2'   'WIST'        /
  'P1_1_LIN'   'WIST_OP'     /
  'P1_2_LIN'   'WIST_OP'     /
  'P1_3_LIN'   'WIST_OP'     /
  'P2_1_LIN'   'WIST_OP'     /
  'P2_2_LIN'   'WIST_OP'     /
  'P2_3_LIN'   'WIST_OP'     /
  'P3_1_LIN'   'WIST_OP'     /
  'P3_2_LIN'   'WIST_OP'     /
  'P3_3_LIN'   'WIST_OP'     /
  'P4_1_LIN'   'WIST_OP'     /
  'P4_2_LIN'   'WIST_OP'     /
  'P5_1_LIN'   'WIST_OP'     /
  'P5_2_LIN'   'WIST_OP'     /
  'P5_3_LIN'   'WIST_OP'     /
  'P5_4_LIN'   'WIST_OP'     /
  'P6_1_LIN'   'WIST_OP'     /
  'P6_2_LIN'   'WIST_OP'     /
  'P6_3_LIN'   'WIST_OP'     /
  'P7_1_LIN'   'WIST_OP'     /
  'P8_1_LIN'   'WIST_OP'     /
  'P8_2_LIN'   'WIST_OP'     /
  'P8_3_LIN'   'WIST_OP'     /
  'P9_1_LIN'   'WIST_OP'     /
  'P9_2_LIN'   'WIST_OP'     /
  'PH_1_LIN'   'WIST_OP'     /
  'PH_2_LIN'   'WIST_OP'     /
  'PH_3_LIN'   'WIST_OP'     /
  'W1_1_LIN'   'WIST_WI'     /
  'W1_2_LIN'   'WIST_WI'     /
  'W2_1_LIN'   'WIST_WI'     /
  'W3_1_LIN'   'WIST_WI'     /
  'W4_1_LIN'   'WIST_WI'     /
  'W5_1_LIN'   'WIST_WI'     /
  'W5_2_LIN'   'WIST_WI'     /
  'W6_1_LIN'   'WIST_WI'     /
  'W6_2_LIN'   'WIST_WI'     /
  'W7_1_LIN'   'WIST_WI'     /
  'W8_1_LIN'   'WIST_WI'     /
  'W8_2_LIN'   'WIST_WI'     /
  'W8_3_LIN'   'WIST_WI'     /
  'W9_1_LIN'   'WIST_WI'     /
  'W9_2_LIN'   'WIST_WI'     /
  'WH_1_LIN'   'WIST_WI'     /
  'WH_2_LIN'   'WIST_WI'     /
  'WH_3_LIN'   'WIST_WI'     /
  'CO2_INJ_LIN' 'WIST_CO2'   /
/

BRANPROP
RISERB   FIELD    10         /
SUBS_MPP RISERB   11         /
CENTRAL  SUBS_MPP 9999       /
HANSSEN  CENTRAL  8          /

P1_1_PIP P1_2_PIP  9999  /
P1_3_PIP P1_2_PIP  9999  /
P1_2_PIP P2_1_PIP  4     /
P2_1_PIP CENTRAL   9     /
P2_2_PIP P2_1_PIP  9999  /

P5_2_PIP CENTRAL  6  /
P5_1_PIP P5_2_PIP 9999 /

P4_2_PIP P4_1_PIP  4  /
P4_1_PIP P3_2_PIP  4  /
P3_2_PIP P3_1_PIP  4  /
P3_1_PIP CENTRAL   9  /

P7_1_PIP P6_1_PIP  4  /
P6_1_PIP P6_2_PIP  9999  /
P6_2_PIP CENTRAL   6  /

P9_1_PIP P9_2_PIP  9999  /
P9_2_PIP P8_3_PIP  4  /
P8_3_PIP P8_2_PIP  6  /
P8_2_PIP CENTRAL   5  /

P8_1_PIP CENTRAL   9999 /

P1_1_LIN P1_1_PIP  9999  /
P1_2_LIN P1_2_PIP  9999  /
P1_3_LIN P1_3_PIP  9999  /
P2_1_LIN P2_1_PIP  9999  /
P2_2_LIN P2_2_PIP  9999  /
P3_1_LIN P3_1_PIP  9999  /
P3_2_LIN P3_2_PIP  9999  /
P4_1_LIN P4_1_PIP  9999  /
P4_2_LIN P4_2_PIP  9999  /
P5_1_LIN P5_1_PIP  9999  /
P5_2_LIN P5_2_PIP  9999  /
P6_1_LIN P6_1_PIP  9999  /
P6_2_LIN P6_2_PIP  9999  /
P7_1_LIN P7_1_PIP  9999  /
P8_1_LIN P8_1_PIP  9999  /
P8_2_LIN P8_2_PIP  9999  /
P8_3_LIN P8_3_PIP  9999  /
P9_1_LIN P9_1_PIP  9999  /
P9_2_LIN P9_2_PIP  9999  /
PH_1_LIN HANSSEN   9999  /
PH_2_LIN HANSSEN   9999  /
PH_3_LIN HANSSEN   9999  /
/

NODEPROP
FIELD   16  2* /
P1_1_LIN 3* /
P1_2_LIN 3* /
P1_3_LIN 3* /
P2_1_LIN 3* /
P2_2_LIN 3* /
P3_1_LIN 3* /
P3_2_LIN 3* /
P4_1_LIN 3* /
P4_2_LIN 3* /
P5_1_LIN 3* /
P5_2_LIN 3* /
P6_1_LIN 3* /
P6_2_LIN 3* /
P7_1_LIN 3* /
P8_1_LIN 3* /
P8_2_LIN 3* /
P8_3_LIN 3* /
P9_1_LIN 3* /
P9_2_LIN 3* /
PH_1_LIN 3* /
PH_2_LIN 3* /
PH_3_LIN 3* /
/
