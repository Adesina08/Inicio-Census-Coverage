import type { Feature, FeatureCollection, MultiPolygon, Point, Polygon } from 'geojson'

export type CoverageStatus = 'well_covered' | 'near_target' | 'under_covered' | 'no_gps'
export type AreaGeometry = Polygon | MultiPolygon

export type StateProperties = {
  stateName: string
  stateCode: string
  capitalCity: string
  geoZone: string
  hasObservations: boolean
  observationCount: number
}

export type LgaProperties = {
  id: string
  stateName: string
  stateCode: string
  lgaName: string
  lgaCode: string
  lgaKey: string
  hasObservations: boolean
  observationCount: number
}

export type WardProperties = {
  id: string
  stateName: string
  stateCode: string
  lgaName: string
  lgaCode: string
  wardName: string
  wardCode: string
  wardKey: string
  urbanClass: string | null
  hasObservations: boolean
  observationCount: number
  rawObservationCount: number
  coveragePercent: number
  coveredCells: number
  totalCells: number
  coveredAreaM2: number
  uncoveredAreaM2: number
  averageAccuracyM: number | null
  coverageStatus: CoverageStatus
}

export type ObservationProperties = {
  id: string
  stateName: string
  lgaName: string
  wardName: string
  wardCode: string
  wardKey: string
  collectorName: string
  deviceId: string
  status: string
  statusDetail: string
  outletType: string
  productCategoryCodes: string[]
  productCategories: string[]
  channelType: string
  businessName: string
  preApproval: boolean
  gpsAccuracy: number
  gpsQualityFlag: string
  effectiveToleranceM: number
  eventTs: string | null
  submissionTs: string | null
  startTime: string | null
  endTime: string | null
  surveyDate: string | null
  reviewState: string
}

export type StateFeature = Feature<AreaGeometry, StateProperties>
export type LgaFeature = Feature<AreaGeometry, LgaProperties>
export type WardFeature = Feature<AreaGeometry, WardProperties>
export type ObservationFeature = Feature<Point, ObservationProperties>

export type StateCollection = FeatureCollection<AreaGeometry, StateProperties>
export type LgaCollection = FeatureCollection<AreaGeometry, LgaProperties>
export type WardCollection = FeatureCollection<AreaGeometry, WardProperties>
export type ObservationCollection = FeatureCollection<Point, ObservationProperties>
export type ClusterPointProperties = {
  id: string
  isCluster: true
  pointCount: number
}
export type ClusterPointFeature = Feature<Point, ClusterPointProperties>
export type MapPointFeature = ObservationFeature | ClusterPointFeature
export type MapPointCollection = {
  type: 'FeatureCollection'
  features: MapPointFeature[]
}

export type OutletTypeAnalysisRow = {
  outletType: string
  count: number
  completedCount: number
  observationCount: number
  sharePercent: number
  stateCount: number
  lgaCount: number
  wardCount: number
  distinctCategoryCount: number
  categoriesSummary: string
}

export type OutletCategoryAnalysisRow = {
  categoryName: string
  count: number
  completedCount: number
  observationCount: number
  sharePercent: number
  stateCount: number
  lgaCount: number
  wardCount: number
}

export type OutletSubcategoryAnalysisRow = {
  categoryName: string
  subcategoryName: string
  count: number
  completedCount: number
  observationCount: number
  sharePercent: number
  stateCount: number
  lgaCount: number
  wardCount: number
}

export type OutletAnalysisData = {
  stateOptions: string[]
  lgaOptions: string[]
  categoryOptions: string[]
  scopeRecordCount: number
  filteredRecordCount: number
  outletTypeRows: OutletTypeAnalysisRow[]
  outletCategoryRows: OutletCategoryAnalysisRow[]
  outletSubcategoryRows: OutletSubcategoryAnalysisRow[]
}

export type DashboardSummary = {
  totalAchieved: number
  completedCount: number
  observationCount: number
  wardsVisitedCount: number
  lgasVisitedCount: number
  validGpsCount: number
  frontendObservationCount?: number
  observationsSampled?: boolean
  generatedAt: string
}

export type DatasetOption = {
  id: string
  label: string
  sourceFile: string
  generatedAt: string
}

export type DashboardData = {
  datasetOptions: DatasetOption[]
  activeDataset: DatasetOption
  states: StateCollection
  lgas: LgaCollection
  wards: WardCollection
  observations: ObservationCollection
  summary: DashboardSummary
  stateByName: Map<string, StateFeature>
  lgaByKey: Map<string, LgaFeature>
  wardByKey: Map<string, WardFeature>
  pointsByWardKey: Map<string, ObservationFeature[]>
}

type MetricsFeature<TProperties> = {
  type: 'Feature'
  id?: string | number
  properties: TProperties
  geometry?: AreaGeometry
}

type MetricsCollection<TProperties> = {
  type: 'FeatureCollection'
  features: MetricsFeature<TProperties>[]
}

type DashboardResponse = {
  datasetOptions: DatasetOption[]
  activeDataset: DatasetOption
  states: MetricsCollection<StateProperties>
  lgas: MetricsCollection<LgaProperties>
  wards: MetricsCollection<WardProperties>
  observations: ObservationCollection
  summary: DashboardSummary
}

async function fetchJson<T>(
  path: string,
  options?: {
    timeoutMs?: number
  },
): Promise<T> {
  const controller = new AbortController()
  const timeoutMs = options?.timeoutMs ?? 30000
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs)
  let response: Response

  try {
    response = await fetch(path, {
      signal: controller.signal,
      cache: 'no-store',
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error(
        `Timed out loading ${path}. The backend may still be initializing runtime tables from the DuckDB files.`,
      )
    }

    throw error
  } finally {
    window.clearTimeout(timeoutId)
  }

  if (!response.ok) {
    let errorDetail = ''
    try {
      const responseBody = (await response.json()) as { error?: string }
      if (typeof responseBody?.error === 'string' && responseBody.error.trim()) {
        errorDetail = `: ${responseBody.error.trim()}`
      }
    } catch {
      // Ignore non-JSON error bodies and fall back to the HTTP status only.
    }

    throw new Error(`Failed to load ${path} (${response.status})${errorDetail}`)
  }

  return (await response.json()) as T
}

const RAW_API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api'

function resolveApiBase() {
  if (/^https?:\/\//i.test(RAW_API_BASE)) {
    return RAW_API_BASE.replace(/\/$/, '')
  }

  if (typeof window === 'undefined') {
    return RAW_API_BASE
  }

  const normalizedPath = RAW_API_BASE.startsWith('/') ? RAW_API_BASE : `/${RAW_API_BASE}`
  if (import.meta.env.DEV) {
    const host =
      window.location.hostname === 'localhost' ? '127.0.0.1' : window.location.hostname
    return `${window.location.protocol}//${host}:8000${normalizedPath}`
  }

  return `${window.location.origin.replace(/\/$/, '')}${normalizedPath}`
}

const API_BASE = resolveApiBase()
const dashboardCache = new Map<string, DashboardData>()
const analysisObservationCache = new Map<string, ObservationCollection>()
const mapPointCache = new Map<string, MapPointCollection>()

function buildAnalysisObservationCacheKey(params: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
}) {
  const searchParams = new URLSearchParams()
  if (params.datasetId) {
    searchParams.set('dataset', params.datasetId)
  }
  if (params.stateName && params.stateName !== 'all') {
    searchParams.set('state', params.stateName)
  }
  if (params.lgaName && params.lgaName !== 'all') {
    searchParams.set('lga', params.lgaName)
  }
  if (params.wardKey) {
    searchParams.set('wardKey', params.wardKey)
  }

  return searchParams.toString() || resolveDashboardCacheKey(params.datasetId)
}
const outletAnalysisCache = new Map<string, OutletAnalysisData>()

let geometryCache:
  | {
      states: StateCollection
      lgas: LgaCollection
      wards: WardCollection
    }
  | null = null

function mergeCollectionGeometry<TProperties, TFeature extends Feature<AreaGeometry, TProperties>>(
  metrics: MetricsCollection<TProperties>,
  geometryCollection: FeatureCollection<AreaGeometry, TProperties>,
  getKey: (properties: TProperties) => string,
): FeatureCollection<AreaGeometry, TProperties> {
  const geometryByKey = new Map(
    geometryCollection.features.map((feature) => [getKey(feature.properties), feature]),
  )

  return {
    type: 'FeatureCollection',
    features: metrics.features.map((feature) => {
      const geometryFeature = geometryByKey.get(getKey(feature.properties))
      if (!geometryFeature) {
        throw new Error(`Missing cached geometry for ${getKey(feature.properties)}`)
      }

      return {
        ...geometryFeature,
        id: feature.id ?? geometryFeature.id,
        properties: feature.properties,
      } as TFeature
    }),
  }
}

function buildDashboardData(
  states: StateCollection,
  lgas: LgaCollection,
  wards: WardCollection,
  observations: ObservationCollection,
  summary: DashboardSummary,
  datasetOptions: DatasetOption[],
  activeDataset: DatasetOption,
): DashboardData {
  const pointsByWardKey = observations.features.reduce((grouped, feature) => {
    const existing = grouped.get(feature.properties.wardKey) ?? []
    existing.push(feature)
    grouped.set(feature.properties.wardKey, existing)
    return grouped
  }, new Map<string, ObservationFeature[]>())

  return {
    datasetOptions,
    activeDataset,
    states,
    lgas,
    wards,
    observations,
    summary,
    stateByName: new Map(states.features.map((feature) => [feature.properties.stateName, feature])),
    lgaByKey: new Map(lgas.features.map((feature) => [feature.properties.lgaKey, feature])),
    wardByKey: new Map(wards.features.map((feature) => [feature.properties.wardKey, feature])),
    pointsByWardKey,
  }
}

function resolveDashboardCacheKey(datasetId?: string) {
  return datasetId ?? '__default__'
}

export function buildOutletAnalysisCacheKey(params: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
  categoryName?: string
  outletType?: string
  outletTypes?: readonly string[]
}) {
  const searchParams = new URLSearchParams()
  if (params.datasetId) {
    searchParams.set('dataset', params.datasetId)
  }
  if (params.stateName && params.stateName !== 'all') {
    searchParams.set('state', params.stateName)
  }
  if (params.lgaName && params.lgaName !== 'all') {
    searchParams.set('lga', params.lgaName)
  }
  if (params.wardKey) {
    searchParams.set('wardKey', params.wardKey)
  }
  if (params.categoryName && params.categoryName !== 'all') {
    searchParams.set('category', params.categoryName)
  }
  if (params.outletTypes && params.outletTypes.length > 0) {
    const normalizedOutletTypes = Array.from(
      new Set(params.outletTypes.map((value) => value.trim()).filter((value) => value.length > 0)),
    ).sort((first, second) => first.localeCompare(second))

    if (normalizedOutletTypes.includes('all') || normalizedOutletTypes.length === 0) {
      return searchParams.toString()
    }

    if (normalizedOutletTypes.length === 1) {
      const outletType = normalizedOutletTypes[0]
      if (outletType !== 'all') {
        searchParams.set('outletType', outletType)
      }
    } else {
      for (const outletType of normalizedOutletTypes) {
        searchParams.append('outletTypes', outletType)
      }
    }
  } else if (params.outletType && params.outletType !== 'all') {
    searchParams.set('outletType', params.outletType)
  }

  return searchParams.toString()
}

// function isDefaultOutletAnalysisScope(params: {
//   datasetId?: string
//   stateName?: string
//   lgaName?: string
//   wardKey?: string
//   categoryName?: string
//   outletType?: string
//   outletTypes?: readonly string[]
// }) {
//   const normalizedOutletTypes = params.outletTypes
//     ? Array.from(
//         new Set(params.outletTypes.map((value) => value.trim()).filter((value) => value.length > 0)),
//       ).sort((first, second) => first.localeCompare(second))
//     : []

//   return (
//     (!params.stateName || params.stateName === 'all') &&
//     (!params.lgaName || params.lgaName === 'all') &&
//     !params.wardKey &&
//     (!params.categoryName || params.categoryName === 'all') &&
//     (!params.outletType || params.outletType === 'all') &&
//     (normalizedOutletTypes.length === 0 ||
//       (normalizedOutletTypes.length === 1 && normalizedOutletTypes[0] === 'all'))
//   )
// }

export function peekDashboardData(datasetId?: string): DashboardData | null {
  return dashboardCache.get(resolveDashboardCacheKey(datasetId)) ?? null
}

export function peekAnalysisObservations(params?: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
}): ObservationCollection | null {
  return analysisObservationCache.get(buildAnalysisObservationCacheKey(params ?? {})) ?? null
}

export function peekOutletAnalysis(params: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
  categoryName?: string
  outletType?: string
  outletTypes?: readonly string[]
}): OutletAnalysisData | null {
  return outletAnalysisCache.get(buildOutletAnalysisCacheKey(params)) ?? null
}

export async function loadDashboardData(datasetId?: string): Promise<DashboardData> {
  const cacheKey = resolveDashboardCacheKey(datasetId)
  const cachedDashboard = dashboardCache.get(cacheKey)
  if (cachedDashboard) {
    return cachedDashboard
  }

  const includeGeometry = geometryCache == null
  const search = datasetId ? `?dataset=${encodeURIComponent(datasetId)}` : ''
  const separator = search ? '&' : '?'
  const payload = await fetchJson<DashboardResponse>(
    `${API_BASE}/dashboard${search}${separator}includeGeometry=${includeGeometry ? '1' : '0'}&includeObservations=0`,
    { timeoutMs: 180000 },
  )

  const states = includeGeometry
    ? (payload.states as StateCollection)
    : mergeCollectionGeometry<StateProperties, StateFeature>(
        payload.states,
        geometryCache!.states,
        (properties) => properties.stateName,
      )
  const lgas = includeGeometry
    ? (payload.lgas as LgaCollection)
    : mergeCollectionGeometry<LgaProperties, LgaFeature>(
        payload.lgas,
        geometryCache!.lgas,
        (properties) => properties.lgaKey,
      )
  const wards = includeGeometry
    ? (payload.wards as WardCollection)
    : mergeCollectionGeometry<WardProperties, WardFeature>(
        payload.wards,
        geometryCache!.wards,
        (properties) => properties.wardKey,
      )

  if (includeGeometry) {
    geometryCache = { states, lgas, wards }
  }

  const dashboard = buildDashboardData(
    states,
    lgas,
    wards,
    payload.observations,
    payload.summary,
    payload.datasetOptions,
    payload.activeDataset,
  )

  dashboardCache.set(payload.activeDataset.id, dashboard)
  dashboardCache.set(cacheKey, dashboard)

  return dashboard
}

export async function loadAnalysisObservations(params?: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
}): Promise<ObservationCollection> {
  const cacheKey = buildAnalysisObservationCacheKey(params ?? {})
  const cached = analysisObservationCache.get(cacheKey)
  if (cached) {
    return cached
  }

  const searchParams = new URLSearchParams()
  if (params?.datasetId) {
    searchParams.set('dataset', params.datasetId)
  }
  if (params?.stateName && params.stateName !== 'all') {
    searchParams.set('state', params.stateName)
  }
  if (params?.lgaName && params.lgaName !== 'all') {
    searchParams.set('lga', params.lgaName)
  }
  if (params?.wardKey) {
    searchParams.set('wardKey', params.wardKey)
  }

  const hasScopeFilter =
    (params?.stateName && params.stateName !== 'all') ||
    (params?.lgaName && params.lgaName !== 'all') ||
    Boolean(params?.wardKey)

  if (!hasScopeFilter) {
    const search = params?.datasetId ? `?dataset=${encodeURIComponent(params.datasetId)}` : ''
    const separator = search ? '&' : '?'
    const payload = await fetchJson<DashboardResponse>(
      `${API_BASE}/dashboard${search}${separator}includeGeometry=0&includeObservations=1`,
      { timeoutMs: 180000 },
    )

    analysisObservationCache.set(payload.activeDataset.id, payload.observations)
    analysisObservationCache.set(cacheKey, payload.observations)
    return payload.observations
  }

  const collection = await fetchJson<ObservationCollection>(
    `${API_BASE}/analysis-observations?${searchParams.toString()}`,
    { timeoutMs: 180000 },
  )
  analysisObservationCache.set(cacheKey, collection)
  return collection
}

export async function loadMapPoints(params: {
  datasetId?: string
  bbox: [number, number, number, number]
  zoom: number
  stateName?: string
  lgaName?: string
}): Promise<MapPointCollection> {
  const searchParams = new URLSearchParams()
  if (params.datasetId) {
    searchParams.set('dataset', params.datasetId)
  }
  searchParams.set('bbox', params.bbox.join(','))
  searchParams.set('zoom', String(params.zoom))
  if (params.stateName && params.stateName !== 'all') {
    searchParams.set('state', params.stateName)
  }
  if (params.lgaName && params.lgaName !== 'all') {
    searchParams.set('lga', params.lgaName)
  }

  const cacheKey = searchParams.toString()
  const cached = mapPointCache.get(cacheKey)
  if (cached) {
    return cached
  }

  const collection = await fetchJson<MapPointCollection>(
    `${API_BASE}/map-points?${searchParams.toString()}`,
    { timeoutMs: 60000 },
  )
  mapPointCache.set(cacheKey, collection)
  return collection
}

export async function loadOutletAnalysis(params: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  wardKey?: string
  categoryName?: string
  outletType?: string
  outletTypes?: readonly string[]
}): Promise<OutletAnalysisData> {
  const cacheKey = buildOutletAnalysisCacheKey(params)
  const cached = outletAnalysisCache.get(cacheKey)
  if (cached) {
    return cached
  }

  const query = cacheKey ? `?${cacheKey}` : ''
  const payload = await fetchJson<OutletAnalysisData>(
    `${API_BASE}/outlet-analysis${query}`,
    { timeoutMs: 60000 },
  )

  outletAnalysisCache.set(cacheKey, payload)
  return payload
}

export function buildPointTileUrl(params: {
  datasetId?: string
  stateName?: string
  lgaName?: string
  coverageStatus?: CoverageStatus | 'all'
}) {
  const searchParams = new URLSearchParams()
  if (params.datasetId) {
    searchParams.set('dataset', params.datasetId)
  }
  if (params.stateName && params.stateName !== 'all') {
    searchParams.set('state', params.stateName)
  }
  if (params.lgaName && params.lgaName !== 'all') {
    searchParams.set('lga', params.lgaName)
  }
  if (params.coverageStatus && params.coverageStatus !== 'all') {
    searchParams.set('coverageStatus', params.coverageStatus)
  }

  const query = searchParams.toString()
  return `${API_BASE}/tiles/{z}/{x}/{y}.mvt${query ? `?${query}` : ''}`
}

export function hasWardObservations(feature: WardFeature) {
  return feature.properties.observationCount > 0
}
