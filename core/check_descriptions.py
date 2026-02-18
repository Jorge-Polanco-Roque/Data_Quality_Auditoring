"""
Descripciones amigables para ejecutivos de todos los checks del framework.
Importar desde aqui en todos los generadores de reportes.
"""

# Titulo corto y claro para cada check (reemplaza el check_id tecnico)
CHECK_FRIENDLY_TITLE = {
    # Universales
    "NULL_RATE": "Datos vacios o faltantes",
    "DUPLICATE_ROWS": "Filas duplicadas",
    "WHITESPACE_ISSUES": "Espacios invisibles en los datos",
    "CONSTANT_COLUMN": "Columna con un solo valor",
    "NEAR_CONSTANT": "Columna casi sin variacion",
    # Numericos
    "OUTLIER_IQR": "Valores fuera del rango tipico",
    "OUTLIER_ZSCORE": "Valores extremadamente alejados del promedio",
    "OUTLIER_MODIFIED_Z": "Valores atipicos (metodo robusto)",
    "DISTRIBUTION_SKEW": "Datos muy cargados hacia un lado",
    "DISTRIBUTION_KURTOSIS": "Valores extremos mas frecuentes de lo esperado",
    "NEGATIVE_VALUES": "Valores negativos detectados",
    "ZERO_VALUES": "Proporcion alta de valores en cero",
    "TREND_CHANGE": "Cambio repentino en el promedio reciente",
    "VALUE_RANGE": "Valores en los extremos del rango",
    "VARIANCE_SUDDEN_CHANGE": "Cambio abrupto en la variabilidad",
    "NORMALITY_TEST": "Distribucion no sigue forma de campana",
    # Fechas
    "DATE_NULL_RATE": "Fechas vacias o no interpretables",
    "DATE_FORMAT_MIX": "Mezcla de formatos de fecha",
    "DATE_FUTURE": "Fechas en el futuro",
    "DATE_ANCIENT": "Fechas anteriores a 1900",
    "DATE_SEQUENCE_GAPS": "Huecos en la secuencia de fechas",
    "DATE_DUPLICATES": "Fechas repetidas",
    "DATE_MONOTONICITY": "Fechas fuera de orden cronologico",
    "DATE_INVALID_PARSED": "Valores que no son fechas validas",
    "TEMPORAL_DRIFT": "Comportamiento diferente entre inicio y fin del periodo",
    # Categoricos
    "RARE_CATEGORIES": "Categorias con muy pocos registros",
    "CARDINALITY_CHANGE": "Numero de opciones distintas",
    "CASE_INCONSISTENCY": "Misma categoria escrita diferente",
    "ENCODING_ANOMALY": "Caracteres extranos o ilegibles",
    "CLASS_IMBALANCE": "Una opcion domina casi todos los registros",
    "TYPO_CANDIDATES": "Posibles errores de dedo en categorias",
    # Texto
    "EMAIL_FORMAT": "Emails con formato invalido",
    "PHONE_FORMAT": "Telefonos con formato inesperado",
    "LENGTH_OUTLIERS": "Textos inusualmente cortos o largos",
    "NULL_LIKE_STRINGS": "Textos que simulan estar vacios",
    "TRUNCATION_SIGNS": "Textos que parecen cortados",
    # IDs
    "ID_DUPLICATES": "Identificadores duplicados",
    "ID_FORMAT_CONSISTENCY": "Formato inconsistente de IDs",
    "ID_NULL": "Identificadores vacios",
    # Hipotesis numericas
    "NORMALITY_ANDERSON": "Prueba de distribucion tipo campana",
    "NORMALITY_LILLIEFORS": "Prueba complementaria de distribucion",
    "MEAN_SHIFT": "El promedio cambio entre la primera y segunda mitad",
    "WILCOXON_PAIRED": "Diferencia significativa en comparacion pareada",
    "VARIANCE_SHIFT": "La dispersion de los datos cambio",
    "KS_GOODNESS_FIT": "Los datos no se ajustan a la distribucion esperada",
    "ADF_STATIONARITY": "Tendencia detectada (datos no estables en el tiempo)",
    # Hipotesis categoricas
    "CHI2_INDEPENDENCE": "Asociacion entre dos columnas categoricas",
    "KRUSKAL_WALLIS": "Valores numericos diferentes entre grupos",
    # Benford
    "BENFORD_LAW": "Distribucion del primer digito no sigue patron natural",
    # PII
    "PII_DETECTED": "Datos personales sensibles expuestos",
    # Cross-column
    "HIGH_CORRELATION": "Dos columnas casi identicas",
    "MULTICOLLINEARITY_VIF": "Columna predecible a partir de otras",
    "CATEGORICAL_ASSOCIATION": "Columnas categoricas fuertemente asociadas",
    "POINT_BISERIAL": "Relacion entre numero y variable Si/No",
    # Patrones de nulos
    "NULL_CORRELATION": "Datos vacios correlacionados entre columnas",
    "NULL_ROW_PATTERN": "Filas casi completamente vacias",
    "MCAR_VIOLATION": "Datos vacios con patron sistematico",
    # Series de tiempo
    "AUTOCORRELATION": "Valores dependen de periodos anteriores",
    "SEASONALITY": "Patron ciclico detectado",
    "CHANGEPOINT_CUSUM": "Punto de quiebre en el comportamiento",
    # Completitud temporal
    "TEMPORAL_COMPLETENESS": "Mas datos faltantes en ciertos periodos",
    "TEMPORAL_NULL_CONCENTRATION": "Datos vacios concentrados en pocos periodos",
    # Schema
    "SCHEMA_MISSING_COLUMN": "Falta una columna esperada",
    "SCHEMA_EXTRA_COLUMNS": "Columnas no definidas en la estructura",
    "SCHEMA_TYPE_MISMATCH": "Tipo de dato no coincide con lo esperado",
    "SCHEMA_NOT_NULL": "Valores vacios en columna obligatoria",
    "SCHEMA_UNIQUE_VIOLATION": "Duplicados en columna que deberia ser unica",
    "SCHEMA_RANGE_VIOLATION": "Valores fuera del rango permitido",
    "SCHEMA_ALLOWED_VALUES": "Valores no incluidos en opciones validas",
    "SCHEMA_PATTERN_VIOLATION": "Formato no cumple con el patron esperado",
    "COMPOSITE_KEY_VIOLATION": "Llave compuesta duplicada",
    # Business rules
    "BUSINESS_RULE": "Violacion de regla de negocio",
}

# Explicacion de impacto de negocio para cada check
CHECK_BUSINESS_IMPACT = {
    # Universales
    "NULL_RATE": "Datos vacios significan informacion perdida. Los promedios, conteos y reportes basados en esta columna pueden ser enganosos o incompletos.",
    "DUPLICATE_ROWS": "Las filas duplicadas inflan conteos, sumas y promedios. Por ejemplo, si una venta aparece dos veces, los ingresos se reportan de mas.",
    "WHITESPACE_ISSUES": "Los espacios invisibles causan que filtros y agrupaciones fallen silenciosamente. 'Mexico' y 'Mexico ' se cuentan como categorias distintas.",
    "CONSTANT_COLUMN": "Una columna donde todos los valores son iguales no aporta informacion util. Puede indicar un error de extraccion o un campo obsoleto.",
    "NEAR_CONSTANT": "Un solo valor domina casi toda la columna. Aporta muy poca informacion para analisis o segmentacion.",
    # Numericos
    "OUTLIER_IQR": "Los valores fuera del rango tipico pueden ser errores de captura, fraudes, o casos excepcionales que merecen revision. Si no se investigan, distorsionan promedios y sumas.",
    "OUTLIER_ZSCORE": "Hay valores extremadamente alejados del promedio general. Pueden sesgar cualquier calculo estadistico basado en esta columna.",
    "OUTLIER_MODIFIED_Z": "Se detectaron valores atipicos con un metodo que funciona bien incluso cuando los datos estan sesgados. Son candidatos prioritarios para revision manual.",
    "DISTRIBUTION_SKEW": "Los datos estan muy cargados hacia un lado. El promedio no representa bien al grupo. Es mas confiable usar la mediana para decisiones.",
    "DISTRIBUTION_KURTOSIS": "Los valores extremos son mas frecuentes de lo esperado. En terminos de riesgo, esto significa que habra mas sorpresas de las anticipadas.",
    "NEGATIVE_VALUES": "Se encontraron valores negativos. En columnas como 'monto' o 'edad' suelen ser errores. En 'saldo' o 'variacion' pueden ser normales.",
    "ZERO_VALUES": "Un porcentaje elevado de ceros puede indicar registros incompletos, errores de carga, o transacciones fallidas que no se limpiaron.",
    "TREND_CHANGE": "El promedio reciente es significativamente diferente al historico. Algo cambio: un nuevo proceso, error de sistema, o cambio de politica que necesita investigacion.",
    "VALUE_RANGE": "Se detectaron valores en los extremos absolutos del rango. Pueden ser legitimos o errores; se recomienda revision manual de los casos.",
    "VARIANCE_SUDDEN_CHANGE": "La variabilidad de los datos cambio abruptamente. La primera mitad se comporta muy diferente a la segunda, lo que sugiere un cambio en el proceso o fuente.",
    "NORMALITY_TEST": "Los datos no siguen una distribucion tipo campana. Es informativo: algunos metodos de analisis pueden requerir ajustes. No es un problema en si mismo.",
    # Fechas
    "DATE_NULL_RATE": "Fechas vacias impiden ubicar eventos en el tiempo. Afecta reportes mensuales, anuales, tendencias y cualquier analisis temporal.",
    "DATE_FORMAT_MIX": "Los formatos mezclados causan que '01/02' se interprete como enero 2 o febrero 1, segun la region. Puede generar errores graves en reportes temporales.",
    "DATE_FUTURE": "Fechas posteriores a hoy generalmente indican errores de captura. Una venta con fecha futura no deberia existir.",
    "DATE_ANCIENT": "Fechas anteriores a 1900 probablemente son errores de captura o valores placeholder como '01/01/1900' usados en lugar de dejar vacio.",
    "DATE_SEQUENCE_GAPS": "Hay periodos sin datos cuando se esperaba continuidad. Puede indicar que un sistema dejo de reportar o que la extraccion perdio registros.",
    "DATE_DUPLICATES": "En datos con fechas unicas (como cierre diario), las repeticiones indican duplicacion de datos.",
    "DATE_MONOTONICITY": "Las fechas no estan en orden cronologico. Esto puede causar errores en calculos de diferencias, tendencias y acumulados.",
    "DATE_INVALID_PARSED": "Hay valores que parecen fecha pero no se pudieron interpretar. Ejemplo: '31/02/2024' o 'fecha_pendiente'. Indica problemas en la fuente.",
    "TEMPORAL_DRIFT": "Los datos del inicio del periodo se comportan diferente a los del final. Puede ser un cambio real de mercado o un problema en la captura de datos.",
    # Categoricos
    "RARE_CATEGORIES": "Categorias que aparecen en muy pocos registros pueden ser errores de captura o datos que deberian agruparse con otra categoria.",
    "CARDINALITY_CHANGE": "Se reporta el numero de opciones distintas en esta columna. Si un campo 'tipo' tiene 500 opciones, puede ser texto libre en lugar de una categoria controlada.",
    "CASE_INCONSISTENCY": "La misma categoria escrita con diferente capitalizacion fragmenta los conteos. Un reporte por region mostraria 'Mexico' tres veces con cifras parciales.",
    "ENCODING_ANOMALY": "Los caracteres extranos indican problemas de codificacion en la fuente. Los datos se ven corruptos y los filtros no funcionan correctamente.",
    "CLASS_IMBALANCE": "Una opcion domina casi todos los registros. La columna aporta poca informacion. Tambien puede indicar que no se estan capturando los otros estados.",
    "TYPO_CANDIDATES": "Se detectaron categorias muy similares que podrian ser el mismo valor escrito diferente. Cada variante se cuenta por separado en reportes.",
    # Texto
    "EMAIL_FORMAT": "Emails con formato invalido significan que no se puede contactar al cliente. Afecta campanas de marketing y comunicaciones.",
    "PHONE_FORMAT": "Telefonos mal formateados no se pueden usar para contacto, SMS marketing o verificacion de identidad.",
    "LENGTH_OUTLIERS": "Textos con longitud muy inusual probablemente son errores de captura o datos pegados en el campo equivocado.",
    "NULL_LIKE_STRINGS": "Textos como 'N/A', 'null' o '-' no se cuentan como vacios pero tampoco son datos reales. Inflan falsamente el conteo de registros 'completos'.",
    "TRUNCATION_SIGNS": "La fuente de datos tiene un limite de caracteres y esta cortando informacion. Direcciones, nombres o descripciones pueden estar incompletas.",
    # IDs
    "ID_DUPLICATES": "Un ID deberia ser unico. Las repeticiones pueden indicar registros duplicados que inflan todas las metricas del negocio.",
    "ID_FORMAT_CONSISTENCY": "Los formatos inconsistentes dificultan cruces entre tablas y pueden causar que relaciones entre datos no hagan match.",
    "ID_NULL": "Un registro sin identificador no se puede rastrear, cruzar con otras tablas, ni desduplicar. Es un problema critico de integridad.",
    # Hipotesis
    "NORMALITY_ANDERSON": "Prueba que verifica si los datos siguen una distribucion tipo campana. El resultado determina que metodos de analisis son validos.",
    "NORMALITY_LILLIEFORS": "Prueba complementaria de normalidad. Util cuando no se conocen las caracteristicas de los datos de antemano.",
    "MEAN_SHIFT": "El promedio cambio significativamente entre la primera y segunda mitad de los datos. Sugiere un cambio en el proceso, error de sistema o estacionalidad.",
    "WILCOXON_PAIRED": "Se detecto una diferencia significativa al comparar datos pareados. Valida incluso cuando los datos no siguen forma de campana.",
    "VARIANCE_SHIFT": "Si antes los valores eran estables y ahora varian mucho (o viceversa), algo cambio en el proceso que genera los datos.",
    "KS_GOODNESS_FIT": "Los datos no se ajustan a la distribucion teorica esperada. Es informativo para elegir los metodos de analisis correctos.",
    "ADF_STATIONARITY": "Los datos muestran una tendencia en el tiempo. Los promedios historicos pueden no ser utiles para predecir el futuro.",
    # Categoricas
    "CHI2_INDEPENDENCE": "Dos columnas categoricas estan relacionadas. Cambios en una afectan automaticamente a la otra. Importante para planificacion.",
    "KRUSKAL_WALLIS": "Los valores numericos varian significativamente entre grupos categoricos. Vale la pena segmentar el analisis.",
    # Benford
    "BENFORD_LAW": "En datos financieros reales, el primer digito sigue un patron natural especifico. Si no lo sigue, puede indicar numeros fabricados o manipulados.",
    # PII
    "PII_DETECTED": "Se detectaron datos personales sensibles expuestos. Puede violar regulaciones de privacidad (LFPDPPP, GDPR) y generar riesgos legales y reputacionales.",
    # Cross-column
    "HIGH_CORRELATION": "Dos columnas se mueven juntas casi identicamente. Una podria ser redundante. Incluir ambas en modelos puede generar resultados inestables.",
    "MULTICOLLINEARITY_VIF": "Una columna se puede predecir a partir de las demas. Incluirla en modelos o analisis causa resultados inestables y poco confiables.",
    "CATEGORICAL_ASSOCIATION": "Dos columnas categoricas estan fuertemente asociadas. Solo se necesita una para segmentar. Ejemplo: 'ciudad' y 'codigo_postal'.",
    "POINT_BISERIAL": "Se encontro relacion entre una columna numerica y una de Si/No. Util para validar logica de negocio.",
    # Patrones de nulos
    "NULL_CORRELATION": "Cuando falta dato en una columna, tiende a faltar tambien en otra. Indica un problema compartido en el proceso de captura.",
    "NULL_ROW_PATTERN": "Hay filas donde la mayoria de columnas estan vacias. Son generalmente registros fallidos o pruebas que deberian eliminarse.",
    "MCAR_VIOLATION": "Los datos vacios siguen un patron sistematico, no son aleatorios. Simplemente ignorarlos introduce sesgo en todos los analisis.",
    # Series de tiempo
    "AUTOCORRELATION": "El valor de un periodo depende de los anteriores. Los datos no son independientes y necesitan metodos especiales de analisis.",
    "SEASONALITY": "Se detectaron patrones que se repiten ciclicamente. Comparar periodos sin ajustar estacionalidad lleva a conclusiones erroneas.",
    "CHANGEPOINT_CUSUM": "Se detecto un punto donde el comportamiento de los datos cambio abruptamente. Util para correlacionar con eventos de negocio.",
    # Completitud temporal
    "TEMPORAL_COMPLETENESS": "En algun periodo hay significativamente mas datos faltantes. Los reportes de ese periodo no son comparables con los demas.",
    "TEMPORAL_NULL_CONCENTRATION": "Los datos vacios estan concentrados en pocos periodos. El reporte de esos periodos subreportara la realidad.",
    # Schema
    "SCHEMA_MISSING_COLUMN": "Falta una columna que deberia existir. Todo proceso que dependa de ella va a fallar.",
    "SCHEMA_EXTRA_COLUMNS": "Hay columnas que no estaban en la estructura esperada. Pueden ser nuevas o basura que conviene revisar.",
    "SCHEMA_TYPE_MISMATCH": "El tipo de dato no coincide con el esperado. Ejemplo: 'monto' aparece como texto, probablemente por valores como '$1,000'.",
    "SCHEMA_NOT_NULL": "Hay valores vacios en una columna obligatoria. Los registros afectados estan incompletos.",
    "SCHEMA_UNIQUE_VIOLATION": "Hay duplicados en una columna que deberia tener valores unicos. Puede haber registros repetidos.",
    "SCHEMA_RANGE_VIOLATION": "Hay valores fuera del rango permitido. Ejemplo: una edad de -5 o 200 es evidentemente un error.",
    "SCHEMA_ALLOWED_VALUES": "Hay valores que no estan en la lista de opciones validas. Son datos que no cumplen las reglas de la organizacion.",
    "SCHEMA_PATTERN_VIOLATION": "Hay valores que no cumplen el formato esperado por la organizacion. Ejemplo: un RFC de 8 caracteres cuando deberian ser 13.",
    "COMPOSITE_KEY_VIOLATION": "Hay combinaciones de columnas que deberian ser unicas pero se repiten. Indica lineas duplicadas en el detalle.",
    # Business rules
    "BUSINESS_RULE": "Se violaron reglas de negocio definidas por la organizacion. Ejemplo: un pedido cancelado sin reembolso.",
}

# Traduccion de tipos semanticos
SEMANTIC_TYPE_LABEL = {
    "NUMERIC_CONTINUOUS": "Numero (continuo)",
    "NUMERIC_DISCRETE": "Numero (discreto)",
    "CATEGORICAL": "Categoria",
    "HIGH_CARDINALITY": "Texto variado",
    "BOOLEAN": "Si / No",
    "DATE": "Fecha",
    "DATETIME": "Fecha y hora",
    "EMAIL": "Email",
    "PHONE": "Telefono",
    "ID_CANDIDATE": "Identificador / ID",
    "MIXED": "Mezcla de tipos",
    "EMPTY": "Vacia",
    "CONSTANT": "Constante",
}

# Etiquetas de severidad
SEVERITY_LABEL = {
    "CRITICAL": "Critico — Accion inmediata",
    "HIGH": "Alto — Investigar pronto",
    "MEDIUM": "Medio — Revisar",
    "LOW": "Bajo — Monitorear",
    "INFO": "Informativo",
    "PASS": "Sin problema",
}

SEVERITY_LABEL_SHORT = {
    "CRITICAL": "Critico",
    "HIGH": "Alto",
    "MEDIUM": "Medio",
    "LOW": "Bajo",
    "INFO": "Informativo",
    "PASS": "OK",
}

SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🟢",
    "INFO": "🔵",
    "PASS": "✅",
}

GRADE_EMOJI = {
    "A": "🟢",
    "B": "🔵",
    "C": "🟡",
    "D": "🟠",
    "F": "🔴",
}

GRADE_LABEL = {
    "A": "Excelente — Datos confiables",
    "B": "Buena — Detalles menores por revisar",
    "C": "Regular — Problemas que pueden afectar reportes",
    "D": "Deficiente — Usar con precaucion",
    "F": "Critico — Datos no confiables, limpieza urgente",
}

STAT_LABEL = {
    "mean": "Promedio",
    "median": "Mediana (valor central)",
    "std": "Desviacion tipica (dispersion)",
    "min": "Valor minimo",
    "max": "Valor maximo",
    "skewness": "Asimetria",
    "kurtosis": "Curtosis (peso de extremos)",
    "outliers_IQR": "Valores atipicos (IQR)",
    "outliers_Z": "Valores atipicos (Z-score)",
}


def friendly_title(check_id: str) -> str:
    """Devuelve titulo amigable para un check_id. Fallback: el propio check_id."""
    return CHECK_FRIENDLY_TITLE.get(check_id, check_id)


def business_impact(check_id: str) -> str:
    """Devuelve explicacion de impacto de negocio. Cadena vacia si no hay."""
    return CHECK_BUSINESS_IMPACT.get(check_id, "")


def friendly_type(semantic_type: str) -> str:
    """Traduce tipo semantico a etiqueta amigable."""
    return SEMANTIC_TYPE_LABEL.get(semantic_type, semantic_type)


def friendly_severity(severity: str) -> str:
    """Devuelve etiqueta amigable para severidad."""
    return SEVERITY_LABEL.get(severity, severity)


def severity_short(severity: str) -> str:
    """Devuelve nombre corto para severidad."""
    return SEVERITY_LABEL_SHORT.get(severity, severity)
