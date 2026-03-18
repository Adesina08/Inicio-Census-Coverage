import {
  startTransition,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import type { FeatureCollection, Point, Polygon } from 'geojson'
import {
  buildOutletAnalysisCacheKey,
  buildPointTileUrl,
  hasWardObservations,
  loadAnalysisObservations,
  loadDashboardData,
  loadMapPoints,
  loadOutletAnalysis,
  peekAnalysisObservations,
  peekDashboardData,
  peekOutletAnalysis,
  type AreaGeometry,
  type ClusterPointFeature,
  type CoverageStatus,
  type DashboardData,
  type LgaFeature,
  type MapPointFeature,
  type OutletAnalysisData,
  type OutletCategoryAnalysisRow,
  type OutletSubcategoryAnalysisRow,
  type OutletTypeAnalysisRow,
  type ObservationFeature,
  type WardFeature,
} from './data/coverage'
import {
  geometryContainsPoint,
  getCollectionBounds,
  getFeatureBounds,
} from './lib/coverage'
import CoverageMap from './components/CoverageMap'

type CoverageFilter = 'all' | CoverageStatus
type FocusMode = 'overview' | 'ward' | 'point'
type AnalysisView = 'out_of_boundary' | 'duplicate_gps' | 'uncovered_wards'
type BasemapId = 'light' | 'streets' | 'satellite' | 'terrain' | 'dark'

type FocusPoint = {
  id: string
  latitude: number
  longitude: number
}

type OutOfBoundaryRow = {
  id: string
  businessName: string
  wardKey: string
  wardCode: string
  latitude: number
  longitude: number
  stateName: string
  lgaName: string
  expectedWardName: string
  detectedWardName: string
  detectedLgaName: string
  collectorName: string
  deviceId: string
  status: string
  statusDetail: string
  outletType: string
  channelType: string
  productCategories: string[]
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

type DuplicateGpsRow = {
  id: string
  wardKey: string
  latitude: number
  longitude: number
  duplicateCount: number
  stateName: string
  lgaName: string
  wardName: string
  outletNames: string
}

type DuplicateGpsCaseRow = {
  duplicateGroupId: string
  duplicateCount: number
  id: string
  businessName: string
  latitude: number
  longitude: number
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
  channelType: string
  productCategories: string[]
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

type BasemapLayerDefinition = {
  attribution: string
  url: string
}

type BasemapDefinition = {
  id: BasemapId
  label: string
  description: string
  backgroundColor?: string
  layers: BasemapLayerDefinition[]
}

type OutletAnalysisScope = {
  stateName: string
  lgaName: string
  wardKey: string
  outletTypes: string[]
}

function normalizeOutletTypeSelection(values: string[]) {
  const normalized = Array.from(
    new Set(values.map((value) => value.trim()).filter((value) => value.length > 0)),
  )

  if (normalized.length === 0 || normalized.includes('all')) {
    return ['all']
  }

  return normalized.sort((first, second) => first.localeCompare(second))
}

const numberFormatter = new Intl.NumberFormat('en-US')
const decimalFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 1,
})
const compactFormatter = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
})

const coverageStatusLabels: Record<CoverageStatus, string> = {
  well_covered: 'Well covered',
  near_target: 'Near target',
  under_covered: 'Under target',
  no_gps: 'No GPS',
}

const basemapOptions: BasemapDefinition[] = [
  {
    id: 'light',
    label: 'Light',
    description: 'Offline-safe plain background',
    backgroundColor: '#eef2f7',
    layers: [],
  },
  {
    id: 'streets',
    label: 'Streets',
    description: 'Roads and places',
    layers: [
      {
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
      },
    ],
  },
  {
    id: 'satellite',
    label: 'Satellite',
    description: 'Imagery and structures',
    layers: [
      {
        attribution:
          'Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community',
        url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
      },
    ],
  },
  {
    id: 'terrain',
    label: 'Terrain',
    description: 'Topography and roads',
    layers: [
      {
        attribution:
          'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, <a href="https://viewfinderpanoramas.org">SRTM</a> | Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
        url: 'https://a.tile.opentopomap.org/{z}/{x}/{y}.png',
      },
    ],
  },
  {
    id: 'dark',
    label: 'Dark',
    description: 'Offline-safe dark background',
    backgroundColor: '#0f172a',
    layers: [],
  },
]

function isClusterMapPoint(
  feature: MapPointFeature,
): feature is ClusterPointFeature {
  return 'isCluster' in feature.properties && feature.properties.isCluster === true
}

function escapeHtml(value: unknown) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function buildAdminKey(...parts: Array<string | null | undefined>) {
  return parts.map((value) => String(value ?? '').trim().toLowerCase()).join('::')
}

function formatAttributeValue(value: unknown) {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(', ') : 'N/A'
  }

  if (typeof value === 'number') {
    return numberFormatter.format(value)
  }

  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No'
  }

  const text = String(value ?? '').trim()
  return text || 'N/A'
}

function formatWardFilterLabel(
  feature: WardFeature,
  selectedState: string,
  selectedLga: string,
) {
  if (selectedState === 'all') {
    return `${feature.properties.wardName} · ${feature.properties.lgaName}, ${feature.properties.stateName}`
  }

  if (selectedLga === 'all') {
    return `${feature.properties.wardName} · ${feature.properties.lgaName}`
  }

  return feature.properties.wardName
}

function renderAttributeTooltip(
  title: string,
  attributes: Array<{ label: string; value: unknown }>,
  subtitle?: string,
) {
  const attributeMarkup = attributes
    .map(
      (attribute) => `
        <div class="map-tooltip__row">
          <span>${escapeHtml(attribute.label)}</span>
          <strong>${escapeHtml(formatAttributeValue(attribute.value))}</strong>
        </div>
      `,
    )
    .join('')

  return `
    <div class="map-tooltip">
      <strong class="map-tooltip__title">${escapeHtml(title)}</strong>
      ${subtitle ? `<span class="map-tooltip__subtitle">${escapeHtml(subtitle)}</span>` : ''}
      <div class="map-tooltip__grid">
        ${attributeMarkup}
      </div>
    </div>
  `
}

function formatCoveragePercent(value: number) {
  return `${decimalFormatter.format(value)}%`
}

function formatArea(value: number) {
  return `${compactFormatter.format(value)} m2`
}

function formatMeters(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return 'N/A'
  }

  return `${decimalFormatter.format(value)} m`
}

function formatFilterValue(value: string, fallback: string) {
  return value === 'all' ? fallback : value
}

function formatCsvCell(value: unknown) {
  const text = String(value ?? '')
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`
  }

  return text
}

function slugifyFilePart(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

function downloadCsvFile(filename: string, headers: string[], rows: Array<Array<unknown>>) {
  const csvLines = [
    headers.map((header) => formatCsvCell(header)).join(','),
    ...rows.map((row) => row.map((cell) => formatCsvCell(cell)).join(',')),
  ]
  const blob = new Blob([`\uFEFF${csvLines.join('\r\n')}`], {
    type: 'text/csv;charset=utf-8',
  })
  const downloadUrl = window.URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = downloadUrl
  link.download = filename
  document.body.append(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(downloadUrl)
}

type PolygonLayerStyle = {
  lineColor: string
  lineWidth: number
  lineOpacity: number
  fillColor: string
  fillOpacity: number
}

type LineLayerStyle = {
  lineColor: string
  lineWidth: number
  lineOpacity: number
}

type PointLayerStyle = {
  circleColor: string
  circleOpacity: number
  circleRadius: number
  circleStrokeColor: string
  circleStrokeOpacity: number
  circleStrokeWidth: number
}

type StyledProperties = Record<string, unknown> & {
  popupHtml: string
}

type BasemapTone = 'light' | 'mixed' | 'dark'

function getBasemapTone(basemapId: BasemapId): BasemapTone {
  if (basemapId === 'dark') {
    return 'dark'
  }

  if (basemapId === 'satellite' || basemapId === 'terrain') {
    return 'mixed'
  }

  return 'light'
}

function getStatePolygonStyle(
  isSelected: boolean,
  isObserved: boolean,
  basemapTone: BasemapTone,
): PolygonLayerStyle {
  if (basemapTone === 'mixed') {
    return {
      lineColor: isSelected ? '#0f172a' : '#f8fafc',
      lineWidth: isSelected ? 2.8 : 2.3,
      lineOpacity: 1,
      fillColor: isSelected ? '#8aa7c8' : isObserved ? '#dbeafe' : '#f8fafc',
      fillOpacity: isSelected ? 0.28 : isObserved ? 0.16 : 0.11,
    }
  }

  if (basemapTone === 'dark') {
    return {
      lineColor: isSelected ? '#ffffff' : '#cbd5e1',
      lineWidth: isSelected ? 2.6 : 2.2,
      lineOpacity: 1,
      fillColor: isSelected ? '#60a5fa' : isObserved ? '#93c5fd' : '#64748b',
      fillOpacity: isSelected ? 0.24 : isObserved ? 0.18 : 0.13,
    }
  }

  return {
    lineColor: isSelected ? '#15396d' : '#8f9db6',
    lineWidth: isSelected ? 1.5 : 1.9,
    lineOpacity: 1,
    fillColor: isSelected ? '#b7c2d4' : isObserved ? '#f3f7fc' : '#f7f9fc',
    fillOpacity: isSelected ? 0.16 : isObserved ? 0.06 : 0.03,
  }
}

function getWardPolygonStyle(
  feature: WardFeature,
  activeWardKey: string,
  basemapTone: BasemapTone,
): PolygonLayerStyle {
  const { coveragePercent, coverageStatus, wardKey } = feature.properties
  const isActive = wardKey === activeWardKey
  const selectedLineColor =
    basemapTone === 'light' ? '#15396d' : basemapTone === 'dark' ? '#ffffff' : '#0f172a'
  const activeFillOpacity = basemapTone === 'light' ? 0.06 : basemapTone === 'dark' ? 0.14 : 0.12
  const inactiveBoost = basemapTone === 'light' ? 0.72 : basemapTone === 'dark' ? 1.15 : 1
  const inactiveLineWidth = basemapTone === 'light' ? 2 : 2.4
  const activeLineWidth = basemapTone === 'light' ? 4.2 : 4.8

  if (coverageStatus === 'no_gps') {
    return {
      lineColor: isActive ? selectedLineColor : basemapTone === 'light' ? '#8f9db6' : '#e2e8f0',
      lineWidth: isActive ? activeLineWidth : inactiveLineWidth,
      lineOpacity: 1,
      fillColor: '#d8dde5',
      fillOpacity: isActive ? activeFillOpacity : 0.024 * inactiveBoost,
    }
  }

  if (coverageStatus === 'well_covered') {
    return {
      lineColor: isActive ? selectedLineColor : '#0f8f6b',
      lineWidth: isActive ? activeLineWidth : inactiveLineWidth,
      lineOpacity: 1,
      fillColor: '#9fe3c3',
      fillOpacity: isActive ? activeFillOpacity : 0.05 * inactiveBoost,
    }
  }

  if (coverageStatus === 'near_target') {
    return {
      lineColor: isActive ? selectedLineColor : '#cb8a1b',
      lineWidth: isActive ? activeLineWidth : inactiveLineWidth,
      lineOpacity: 1,
      fillColor: '#f6d89c',
      fillOpacity: isActive ? activeFillOpacity : 0.045 * inactiveBoost,
    }
  }

  return {
    lineColor: isActive ? selectedLineColor : '#d25a2f',
    lineWidth: isActive ? activeLineWidth : inactiveLineWidth,
    lineOpacity: 1,
    fillColor: '#f4b39a',
    fillOpacity:
      isActive
        ? activeFillOpacity
        : Math.max(
            basemapTone === 'light' ? 0.032 : basemapTone === 'dark' ? 0.07 : 0.06,
            Math.min(
              basemapTone === 'light' ? 0.065 : basemapTone === 'dark' ? 0.11 : 0.095,
              (coveragePercent / 38) * inactiveBoost,
            ),
          ),
  }
}

function getLgaLineStyle(
  feature: LgaFeature,
  selectedState: string,
  selectedLga: string,
  basemapTone: BasemapTone,
): LineLayerStyle {
  const isActive =
    selectedLga !== 'all' &&
    feature.properties.stateName === selectedState &&
    feature.properties.lgaName === selectedLga
  const isScoped = selectedState === 'all' || feature.properties.stateName === selectedState

  if (basemapTone === 'mixed') {
    return {
      lineColor: isActive ? '#0f172a' : isScoped ? '#f8fafc' : '#dbe4f0',
      lineWidth: isActive ? 3.4 : isScoped ? 2.1 : 1.6,
      lineOpacity: isActive ? 1 : isScoped ? 0.92 : 0.62,
    }
  }

  if (basemapTone === 'dark') {
    return {
      lineColor: isActive ? '#ffffff' : isScoped ? '#cbd5e1' : '#64748b',
      lineWidth: isActive ? 3.2 : isScoped ? 2 : 1.4,
      lineOpacity: isActive ? 1 : isScoped ? 0.9 : 0.56,
    }
  }

  return {
    lineColor: isActive ? '#15396d' : isScoped ? '#7a8ca8' : '#bcc6d5',
    lineWidth: isActive ? 3 : isScoped ? 1.7 : 1.2,
    lineOpacity: isActive ? 0.95 : isScoped ? 0.72 : 0.42,
  }
}

function getObservationCircleStyle(
  feature: ObservationFeature,
  activeWardKey: string,
): PointLayerStyle {
  const isObservation = feature.properties.status === 'Observation'
  const isActive = feature.properties.wardKey === activeWardKey

  return {
    circleColor: isObservation ? '#e46635' : '#24a16f',
    circleOpacity: isActive ? 0.98 : 0.84,
    circleRadius: isActive ? 8.4 : 6.2,
    circleStrokeColor: 'transparent',
    circleStrokeOpacity: 0,
    circleStrokeWidth: 0,
  }
}

function withStyledProperties<T extends Record<string, unknown>>(
  properties: T,
  additions: StyledProperties,
): T & StyledProperties {
  return {
    ...properties,
    ...additions,
  }
}

function MetricCard({ label, value, tone = 'default' }: { label: string; value: string; tone?: string }) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">{value}</strong>
    </article>
  )
}

function SelectField({
  label,
  value,
  onChange,
  children,
}: {
  label: string
  value: string
  onChange: (value: string) => void
  children: ReactNode
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {children}
      </select>
    </label>
  )
}

function hasDefaultOutletAnalysisScope(scope: OutletAnalysisScope) {
  return (
    scope.stateName === 'all' &&
    scope.lgaName === 'all' &&
    scope.wardKey === '' &&
    scope.outletTypes.length === 1 &&
    scope.outletTypes[0] === 'all'
  )
}

export default function App() {
  const analysisStageRef = useRef<HTMLElement | null>(null)
  const outletStageRef = useRef<HTMLElement | null>(null)
  const hasCompletedInitialLoadRef = useRef(false)
  const outletAnalysisLoadVersionRef = useRef(0)
  const outletAnalysisRequestKeyRef = useRef('')
  const outletAnalysisScopeRef = useRef<OutletAnalysisScope>({
    stateName: 'all',
    lgaName: 'all',
    wardKey: '',
    outletTypes: ['all'],
  })
  const [dashboard, setDashboard] = useState<DashboardData | null>(null)
  const [analysisObservations, setAnalysisObservations] = useState<ObservationFeature[]>([])
  const [outletAnalysis, setOutletAnalysis] = useState<OutletAnalysisData | null>(null)
  const [mapPoints, setMapPoints] = useState<MapPointFeature[]>([])
  const [showStartupLoading, setShowStartupLoading] = useState(true)
  const [isStartupReady, setIsStartupReady] = useState(false)
  const [loadingProgress, setLoadingProgress] = useState(0)
  const [loadingProgressTarget, setLoadingProgressTarget] = useState(0)
  const [loadingPhaseLabel, setLoadingPhaseLabel] = useState('Preparing dashboard structure.')
  const [loadError, setLoadError] = useState<string | null>(null)
  const [isDatasetLoading, setIsDatasetLoading] = useState(false)
  const [isAnalysisLoading, setIsAnalysisLoading] = useState(false)
  const [isOutletAnalysisLoading, setIsOutletAnalysisLoading] = useState(false)
  const [isMapPointsLoading, setIsMapPointsLoading] = useState(false)
  const [shouldLoadBoundaryAnalysis, setShouldLoadBoundaryAnalysis] = useState(false)
  const [shouldLoadOutletAnalysis, setShouldLoadOutletAnalysis] = useState(false)
  const [selectedDatasetId, setSelectedDatasetId] = useState('')
  const [selectedBasemap, setSelectedBasemap] = useState<BasemapId>('light')
  const [selectedState, setSelectedState] = useState('all')
  const [selectedLga, setSelectedLga] = useState('all')
  const [selectedCoverageStatus, setSelectedCoverageStatus] = useState<CoverageFilter>('all')
  const [selectedWardKey, setSelectedWardKey] = useState('')
  const [focusMode, setFocusMode] = useState<FocusMode>('overview')
  const [analysisView, setAnalysisView] = useState<AnalysisView>('out_of_boundary')
  const [selectedAnalysisRowId, setSelectedAnalysisRowId] = useState('')
  const [selectedPoint, setSelectedPoint] = useState<FocusPoint | null>(null)
  const [outletAnalysisState, setOutletAnalysisState] = useState('all')
  const [outletAnalysisLga, setOutletAnalysisLga] = useState('all')
  const [outletAnalysisWardKey, setOutletAnalysisWardKey] = useState('')
  const [outletAnalysisGranularity, setOutletAnalysisGranularity] = useState<
    'category' | 'subcategory'
  >('category')
  const [outletAnalysisView, setOutletAnalysisView] = useState<'table' | 'chart'>('table')
  const [outletAnalysisOutletTypes, setOutletAnalysisOutletTypes] = useState<string[]>(['all'])

  useEffect(() => {
    outletAnalysisScopeRef.current = {
      stateName: outletAnalysisState,
      lgaName: outletAnalysisLga,
      wardKey: outletAnalysisWardKey,
      outletTypes: outletAnalysisOutletTypes,
    }
  }, [
    outletAnalysisLga,
    outletAnalysisOutletTypes,
    outletAnalysisState,
    outletAnalysisWardKey,
  ])

  useEffect(() => {
    if (loadingProgress === loadingProgressTarget) {
      return
    }

    const timeoutId = window.setTimeout(() => {
      setLoadingProgress((current) => {
        if (current === loadingProgressTarget) {
          return current
        }

        if (current > loadingProgressTarget) {
          return loadingProgressTarget
        }

        const delta = loadingProgressTarget - current
        const step = Math.max(1, Math.ceil(delta / 8))
        return Math.min(loadingProgressTarget, current + step)
      })
    }, 24)

    return () => {
      window.clearTimeout(timeoutId)
    }
  }, [loadingProgress, loadingProgressTarget])

  useEffect(() => {
    if (hasCompletedInitialLoadRef.current) {
      return
    }

    if (!isStartupReady || loadError || loadingProgress < 100) {
      return
    }

    const timeoutId = window.setTimeout(() => {
      hasCompletedInitialLoadRef.current = true
      setShowStartupLoading(false)
    }, 120)

    return () => {
      window.clearTimeout(timeoutId)
    }
  }, [isStartupReady, loadError, loadingProgress])

  useEffect(() => {
    let ignore = false
    const isInitialLoad = !hasCompletedInitialLoadRef.current

    setLoadError(null)
    if (isInitialLoad) {
      setIsStartupReady(false)
    }
    const requestedDatasetId = selectedDatasetId || undefined
    const cachedDashboard = peekDashboardData(requestedDatasetId)
    const defaultOutletScope = {
      datasetId: requestedDatasetId,
      stateName: 'all',
      lgaName: 'all',
      outletTypes: ['all'],
    } as const
    const cachedAnalysis = peekAnalysisObservations({ datasetId: requestedDatasetId })
    const cachedOutletAnalysis = peekOutletAnalysis(defaultOutletScope)
    const defaultOutletScopeKey = buildOutletAnalysisCacheKey(defaultOutletScope)
    const isFullyCached =
      cachedDashboard != null && cachedAnalysis != null && cachedOutletAnalysis != null
    const initialProgress = isFullyCached ? 100 : cachedDashboard ? 46 : 0

    if (isInitialLoad) {
      setShowStartupLoading(true)
    }

    setIsDatasetLoading(!isFullyCached)
    setIsAnalysisLoading(cachedAnalysis == null)
    setIsOutletAnalysisLoading(cachedOutletAnalysis == null)
    setIsMapPointsLoading(false)
    setLoadingProgress(initialProgress)
    setLoadingProgressTarget(initialProgress)
    setLoadingPhaseLabel(
      isFullyCached
        ? 'Opening cached dataset.'
        : cachedDashboard
          ? 'Loading analysis tables.'
          : 'Preparing dashboard structure.',
    )
    setShouldLoadBoundaryAnalysis(false)
    setShouldLoadOutletAnalysis(false)
    setAnalysisObservations(cachedAnalysis?.features ?? [])
    outletAnalysisRequestKeyRef.current = defaultOutletScopeKey
    setOutletAnalysis(cachedOutletAnalysis)
    setMapPoints([])

    if (cachedDashboard) {
      startTransition(() => {
        setDashboard(cachedDashboard)
        setLoadError(null)
      })
    } else if (!isFullyCached) {
      setLoadingProgressTarget(12)
    }

    ;(async () => {
      try {
        let nextDashboard = cachedDashboard
        if (!nextDashboard) {
          setLoadingPhaseLabel('Loading boundaries and coverage metrics.')
          setLoadingProgressTarget(58)
          nextDashboard = await loadDashboardData(requestedDatasetId)
        }
        if (ignore) {
          return
        }

        startTransition(() => {
          setDashboard(nextDashboard)
          setLoadError(null)
        })

        if (ignore) {
          return
        }

        if (!isInitialLoad) {
          if (cachedAnalysis) {
            setAnalysisObservations(cachedAnalysis.features)
          }
          if (
            cachedOutletAnalysis &&
            hasDefaultOutletAnalysisScope(outletAnalysisScopeRef.current) &&
            outletAnalysisRequestKeyRef.current === defaultOutletScopeKey
          ) {
            setOutletAnalysis(cachedOutletAnalysis)
          }

          setIsDatasetLoading(false)

          if (cachedAnalysis == null || cachedOutletAnalysis == null) {
            setLoadingPhaseLabel('Loading analysis tables.')
          }

          const analysisPromise =
            cachedAnalysis != null
              ? Promise.resolve(cachedAnalysis)
              : loadAnalysisObservations({ datasetId: requestedDatasetId })
          const outletPromise =
            cachedOutletAnalysis != null
              ? Promise.resolve(cachedOutletAnalysis)
              : loadOutletAnalysis({ datasetId: requestedDatasetId })

          const [analysisResult, outletResult] = await Promise.allSettled([
            analysisPromise,
            outletPromise,
          ])

          if (ignore) {
            return
          }

          startTransition(() => {
            if (analysisResult.status === 'fulfilled') {
              setAnalysisObservations(analysisResult.value.features)
            }
            if (
              outletResult.status === 'fulfilled' &&
              hasDefaultOutletAnalysisScope(outletAnalysisScopeRef.current) &&
              outletAnalysisRequestKeyRef.current === defaultOutletScopeKey
            ) {
              setOutletAnalysis(outletResult.value)
            }
            setIsAnalysisLoading(false)
            setIsOutletAnalysisLoading(false)
          })

          return
        }

        if (cachedAnalysis == null || cachedOutletAnalysis == null) {
          setLoadingPhaseLabel('Loading analysis tables.')
          setLoadingProgressTarget(88)
        }

        const [nextAnalysis, nextOutletAnalysis] = await Promise.all([
          cachedAnalysis ?? loadAnalysisObservations({ datasetId: requestedDatasetId }),
          cachedOutletAnalysis ?? loadOutletAnalysis({ datasetId: requestedDatasetId }),
        ])

        if (ignore) {
          return
        }

        setLoadingProgressTarget(100)
        setLoadingPhaseLabel('Finalizing workspace.')
        await new Promise((resolve) => window.setTimeout(resolve, 120))

        if (ignore) {
          return
        }

        startTransition(() => {
          setAnalysisObservations(nextAnalysis.features)
          if (
            hasDefaultOutletAnalysisScope(outletAnalysisScopeRef.current) &&
            outletAnalysisRequestKeyRef.current === defaultOutletScopeKey
          ) {
            setOutletAnalysis(nextOutletAnalysis)
          }
          setLoadError(null)
          setIsDatasetLoading(false)
          setIsAnalysisLoading(false)
          setIsOutletAnalysisLoading(false)
        })
        setIsStartupReady(true)
      } catch (error: unknown) {
        if (ignore) {
          return
        }

        setIsStartupReady(false)
        setIsDatasetLoading(false)
        setIsAnalysisLoading(false)
        setIsOutletAnalysisLoading(false)
        setLoadingProgressTarget(100)
        setLoadingPhaseLabel('Unable to load dataset analysis.')
        setLoadError(
          error instanceof Error ? error.message : 'Unable to load dataset analysis.'
        )
        if (dashboard?.activeDataset.id) {
          setSelectedDatasetId(dashboard.activeDataset.id)
        }
      }
    })()

    return () => {
      ignore = true
    }
  }, [selectedDatasetId])

  useEffect(() => {
    if (!dashboard || shouldLoadBoundaryAnalysis || !analysisStageRef.current) {
      return
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) {
          return
        }

        setShouldLoadBoundaryAnalysis(true)
        observer.disconnect()
      },
      {
        rootMargin: '320px 0px',
      },
    )

    observer.observe(analysisStageRef.current)

    return () => {
      observer.disconnect()
    }
  }, [dashboard, shouldLoadBoundaryAnalysis])

  useEffect(() => {
    if (!dashboard || shouldLoadOutletAnalysis || !outletStageRef.current) {
      return
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) {
          return
        }

        setShouldLoadOutletAnalysis(true)
        observer.disconnect()
      },
      {
        rootMargin: '320px 0px',
      },
    )

    observer.observe(outletStageRef.current)

    return () => {
      observer.disconnect()
    }
  }, [dashboard, shouldLoadOutletAnalysis])

  useEffect(() => {
    if (!dashboard || !shouldLoadBoundaryAnalysis) {
      return
    }

    const analysisScope = {
      datasetId: dashboard.activeDataset.id,
      stateName: selectedState,
      lgaName: selectedLga,
      wardKey: selectedWardKey || undefined,
    }
    const cachedAnalysis = peekAnalysisObservations(analysisScope)
    if (cachedAnalysis) {
      setAnalysisObservations(cachedAnalysis.features)
      return
    }

    let ignore = false
    const timeoutId = window.setTimeout(() => {
      setIsAnalysisLoading(true)

      loadAnalysisObservations(analysisScope)
        .then((collection) => {
          if (ignore) {
            return
          }

          startTransition(() => {
            setAnalysisObservations(collection.features)
            setIsAnalysisLoading(false)
          })
        })
        .catch(() => {
          if (ignore) {
            return
          }

          setIsAnalysisLoading(false)
        })
    }, 150)

    return () => {
      ignore = true
      window.clearTimeout(timeoutId)
    }
  }, [
    dashboard?.activeDataset.id,
    selectedLga,
    selectedState,
    selectedWardKey,
    shouldLoadBoundaryAnalysis,
  ])

  useEffect(() => {
    if (!dashboard) {
      return
    }

    const outletScope = {
      datasetId: dashboard.activeDataset.id,
      stateName: outletAnalysisState,
      lgaName: outletAnalysisLga,
      wardKey: outletAnalysisWardKey || undefined,
      outletTypes: outletAnalysisOutletTypes,
    } as const
    const requestKey = buildOutletAnalysisCacheKey(outletScope)
    const cachedOutletAnalysis = peekOutletAnalysis(outletScope)
    const requestVersion = ++outletAnalysisLoadVersionRef.current
    outletAnalysisRequestKeyRef.current = requestKey
    if (cachedOutletAnalysis) {
      setOutletAnalysis(cachedOutletAnalysis)
      setIsOutletAnalysisLoading(false)
      return
    }

    let ignore = false
    setIsOutletAnalysisLoading(true)

    loadOutletAnalysis({
      ...outletScope,
    })
      .then((payload) => {
        if (
          ignore ||
          outletAnalysisLoadVersionRef.current !== requestVersion ||
          outletAnalysisRequestKeyRef.current !== requestKey
        ) {
          return
        }

        startTransition(() => {
          setOutletAnalysis(payload)
          setIsOutletAnalysisLoading(false)
        })
      })
      .catch(() => {
        if (ignore || outletAnalysisLoadVersionRef.current !== requestVersion) {
          return
        }

        setIsOutletAnalysisLoading(false)
      })

    return () => {
      ignore = true
    }
  }, [
    dashboard?.activeDataset.id,
    outletAnalysisLga,
    outletAnalysisOutletTypes,
    outletAnalysisState,
    outletAnalysisWardKey,
  ])

  useEffect(() => {
    if (!dashboard || isDatasetLoading) {
      return
    }

    const activeDatasetId = dashboard.activeDataset.id
    const defaultOutletScope = {
      datasetId: activeDatasetId,
      stateName: 'all',
      lgaName: 'all',
      outletTypes: ['all'],
    } as const

    const cachedAnalysis = peekAnalysisObservations({ datasetId: activeDatasetId })
    if (cachedAnalysis) {
      setAnalysisObservations(cachedAnalysis.features)
    }

    const cachedOutletAnalysis = peekOutletAnalysis(defaultOutletScope)
    if (
      cachedOutletAnalysis &&
      hasDefaultOutletAnalysisScope(outletAnalysisScopeRef.current)
    ) {
      // Keep the default-scope data warm in cache; the scoped outlet-analysis effect owns rendered state.
    }

    if (cachedAnalysis && cachedOutletAnalysis) {
      return
    }

    let cancelled = false
    const timeoutId = window.setTimeout(() => {
      ;(async () => {
        if (!cachedAnalysis) {
          setIsAnalysisLoading(true)

          try {
            const collection = await loadAnalysisObservations({ datasetId: activeDatasetId })
            if (cancelled || dashboard.activeDataset.id !== activeDatasetId) {
              return
            }

            startTransition(() => {
              setAnalysisObservations(collection.features)
              setIsAnalysisLoading(false)
            })
          } catch {
            if (!cancelled) {
              setIsAnalysisLoading(false)
            }
          }
        }

        if (!cachedOutletAnalysis) {
          setIsOutletAnalysisLoading(true)

          try {
            await loadOutletAnalysis({ datasetId: activeDatasetId })
            if (cancelled || dashboard.activeDataset.id !== activeDatasetId) {
              return
            }

            startTransition(() => {
              setIsOutletAnalysisLoading(false)
            })
          } catch {
            if (!cancelled) {
              setIsOutletAnalysisLoading(false)
            }
          }
        }
      })()
    }, 480)

    return () => {
      cancelled = true
      window.clearTimeout(timeoutId)
    }
  }, [dashboard?.activeDataset.id])

  useEffect(() => {
    if (!dashboard) {
      return
    }

    const otherDatasets = dashboard.datasetOptions.filter(
      (dataset) => dataset.id !== dashboard.activeDataset.id,
    )
    if (otherDatasets.length === 0) {
      return
    }

    let cancelled = false
    const timeoutId = window.setTimeout(() => {
      ;(async () => {
        for (const dataset of otherDatasets) {
          if (cancelled) {
            return
          }

          if (!peekDashboardData(dataset.id)) {
            try {
              await loadDashboardData(dataset.id)
            } catch {
              return
            }
          }

          if (
            !peekOutletAnalysis({
              datasetId: dataset.id,
              stateName: 'all',
              lgaName: 'all',
              outletTypes: ['all'],
            })
          ) {
            try {
              await loadOutletAnalysis({ datasetId: dataset.id })
            } catch {
              return
            }
          }
        }
      })()
    }, 600)

    return () => {
      cancelled = true
      window.clearTimeout(timeoutId)
    }
  }, [dashboard])

  const allStateFeatures = dashboard?.states.features ?? []
  const allWardFeatures = dashboard?.wards.features ?? []
  const allObservationFeatures = analysisObservations

  const stateOptions = useMemo(
    () =>
      Array.from(
        new Set(
          allStateFeatures
            .filter((feature) => feature.properties.hasObservations)
            .map((feature) => feature.properties.stateName)
            .filter((value) => value.trim().length > 0),
        ),
      ).sort((first, second) => first.localeCompare(second)),
    [allStateFeatures],
  )

  const outletAnalysisStateOptions = outletAnalysis?.stateOptions ?? []

  const wardsByStateLga = useMemo(() => {
    const grouped = new Map<string, WardFeature[]>()

    allWardFeatures.forEach((feature) => {
      const key = buildAdminKey(feature.properties.stateName, feature.properties.lgaName)
      const existing = grouped.get(key) ?? []
      existing.push(feature)
      grouped.set(key, existing)
    })

    return grouped
  }, [allWardFeatures])

  const wardsByState = useMemo(() => {
    const grouped = new Map<string, WardFeature[]>()

    allWardFeatures.forEach((feature) => {
      const key = buildAdminKey(feature.properties.stateName)
      const existing = grouped.get(key) ?? []
      existing.push(feature)
      grouped.set(key, existing)
    })

    return grouped
  }, [allWardFeatures])

  const lgaOptions = useMemo(
    () =>
      Array.from(
        new Set(
          (dashboard?.lgas.features ?? [])
            .filter(
              (feature) =>
                feature.properties.hasObservations &&
                (selectedState === 'all' || feature.properties.stateName === selectedState),
            )
            .map((feature) => feature.properties.lgaName)
            .filter((value) => value.trim().length > 0),
        ),
      ).sort((first, second) => first.localeCompare(second)),
    [dashboard?.lgas.features, selectedState],
  )

  const wardOptions = useMemo(() => {
    return allWardFeatures
      .filter(
        (feature) =>
          feature.properties.rawObservationCount > 0 &&
          (selectedState === 'all' || feature.properties.stateName === selectedState) &&
          (selectedLga === 'all' || feature.properties.lgaName === selectedLga),
      )
      .sort((first, second) => {
        const firstLabel = `${first.properties.wardName} ${first.properties.lgaName} ${first.properties.stateName}`
        const secondLabel = `${second.properties.wardName} ${second.properties.lgaName} ${second.properties.stateName}`

        return firstLabel.localeCompare(secondLabel)
      })
  }, [allWardFeatures, selectedLga, selectedState])

  const outletAnalysisLgaOptions = outletAnalysis?.lgaOptions ?? []
  const outletAnalysisWardOptions = useMemo(() => {
    return allWardFeatures
      .filter(
        (feature) =>
          feature.properties.rawObservationCount > 0 &&
          (outletAnalysisState === 'all' ||
            feature.properties.stateName === outletAnalysisState) &&
          (outletAnalysisLga === 'all' || feature.properties.lgaName === outletAnalysisLga),
      )
      .sort((first, second) => {
        const firstLabel = `${first.properties.wardName} ${first.properties.lgaName} ${first.properties.stateName}`
        const secondLabel = `${second.properties.wardName} ${second.properties.lgaName} ${second.properties.stateName}`

        return firstLabel.localeCompare(secondLabel)
      })
  }, [allWardFeatures, outletAnalysisLga, outletAnalysisState])
  const outletTypeRows: OutletTypeAnalysisRow[] = outletAnalysis?.outletTypeRows ?? []
  const outletCategoryRows: OutletCategoryAnalysisRow[] =
    outletAnalysis?.outletCategoryRows ?? []
  const supportsSubcategoryAnalysis =
    outletAnalysis == null ||
    Object.prototype.hasOwnProperty.call(outletAnalysis, 'outletSubcategoryRows')
  const outletSubcategoryRows: OutletSubcategoryAnalysisRow[] =
    outletAnalysis?.outletSubcategoryRows ?? []
  const outletAnalysisScopeRecordCount = outletAnalysis?.scopeRecordCount ?? 0
  const outletAnalysisFilteredRecordCount =
    outletAnalysis?.filteredRecordCount ?? outletAnalysisScopeRecordCount
  const outletAnalysisActiveOutletTypeLabel =
    outletAnalysisOutletTypes.length === 1 && outletAnalysisOutletTypes[0] === 'all'
      ? 'All outlet types'
      : outletAnalysisOutletTypes.length === 1
        ? outletAnalysisOutletTypes[0]
        : `${outletAnalysisOutletTypes.length} outlet types selected`
  const outletAnalysisTitle =
    outletAnalysisGranularity === 'category'
      ? 'Broad categories by outlet type'
      : 'Sub categories by outlet type'
  const hasOutletAnalysisRows =
    outletAnalysisGranularity === 'category'
      ? outletCategoryRows.length > 0
      : supportsSubcategoryAnalysis && outletSubcategoryRows.length > 0
  const outletAnalysisEmptyMessage =
    outletAnalysisGranularity === 'category'
      ? 'No outlet categories in the current outlet-analysis scope.'
      : supportsSubcategoryAnalysis
        ? 'No outlet sub categories in the current outlet-analysis scope.'
        : 'Sub category analysis is unavailable from the current backend response. Restart the backend and refresh the page.'
  const outletAnalysisTableContent =
    outletAnalysisGranularity === 'category' ? (
      <table className="analysis-table analysis-table--outlet">
        <colgroup>
          <col style={{ width: '22%' }} />
          <col style={{ width: '14%' }} />
          <col style={{ width: '11%' }} />
          <col style={{ width: '13%' }} />
          <col style={{ width: '14%' }} />
          <col style={{ width: '9%' }} />
          <col style={{ width: '9%' }} />
          <col style={{ width: '8%' }} />
        </colgroup>
        <thead>
          <tr>
            <th>Broad category</th>
            <th>GPS records</th>
            <th>Share</th>
            <th>Completed</th>
            <th>Observation</th>
            <th>States</th>
            <th>LGAs</th>
            <th>Wards</th>
          </tr>
        </thead>
        <tbody>
          {outletCategoryRows.map((row) => (
            <tr key={row.categoryName}>
              <td>{row.categoryName}</td>
              <td>{numberFormatter.format(row.count)}</td>
              <td>{`${decimalFormatter.format(row.sharePercent)}%`}</td>
              <td>{numberFormatter.format(row.completedCount)}</td>
              <td>{numberFormatter.format(row.observationCount)}</td>
              <td>{numberFormatter.format(row.stateCount)}</td>
              <td>{numberFormatter.format(row.lgaCount)}</td>
              <td>{numberFormatter.format(row.wardCount)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    ) : (
      <table className="analysis-table analysis-table--outlet">
        <colgroup>
          <col style={{ width: '18%' }} />
          <col style={{ width: '20%' }} />
          <col style={{ width: '12%' }} />
          <col style={{ width: '10%' }} />
          <col style={{ width: '12%' }} />
          <col style={{ width: '12%' }} />
          <col style={{ width: '6%' }} />
          <col style={{ width: '5%' }} />
          <col style={{ width: '5%' }} />
        </colgroup>
        <thead>
          <tr>
            <th>Broad category</th>
            <th>Sub category</th>
            <th>GPS records</th>
            <th>Share</th>
            <th>Completed</th>
            <th>Observation</th>
            <th>States</th>
            <th>LGAs</th>
            <th>Wards</th>
          </tr>
        </thead>
        <tbody>
          {outletSubcategoryRows.map((row) => (
            <tr key={`${row.categoryName}-${row.subcategoryName}`}>
              <td>{row.categoryName}</td>
              <td>{row.subcategoryName}</td>
              <td>{numberFormatter.format(row.count)}</td>
              <td>{`${decimalFormatter.format(row.sharePercent)}%`}</td>
              <td>{numberFormatter.format(row.completedCount)}</td>
              <td>{numberFormatter.format(row.observationCount)}</td>
              <td>{numberFormatter.format(row.stateCount)}</td>
              <td>{numberFormatter.format(row.lgaCount)}</td>
              <td>{numberFormatter.format(row.wardCount)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    )
  const outletAnalysisChartContent =
    outletAnalysisGranularity === 'category'
      ? outletCategoryRows.map((row) => (
          <div key={row.categoryName} className="outlet-chart__row">
            <div className="outlet-chart__meta">
              <strong>{row.categoryName}</strong>
              <span>
                {numberFormatter.format(row.count)} records ·{' '}
                {decimalFormatter.format(row.sharePercent)}%
              </span>
            </div>
            <div className="outlet-chart__track">
              <div
                className="outlet-chart__fill"
                style={{ width: `${Math.min(100, row.sharePercent)}%` }}
              />
            </div>
          </div>
        ))
      : outletSubcategoryRows.map((row) => (
          <div key={`${row.categoryName}-${row.subcategoryName}`} className="outlet-chart__row">
            <div className="outlet-chart__meta">
              <strong>{row.subcategoryName}</strong>
              <span>
                {row.categoryName} · {numberFormatter.format(row.count)} records ·{' '}
                {decimalFormatter.format(row.sharePercent)}%
              </span>
            </div>
            <div className="outlet-chart__track">
              <div
                className="outlet-chart__fill"
                style={{ width: `${Math.min(100, row.sharePercent)}%` }}
              />
            </div>
          </div>
        ))

  const activeBasemap =
    basemapOptions.find((option) => option.id === selectedBasemap) ?? basemapOptions[0]
  const basemapTone = getBasemapTone(selectedBasemap)

  useEffect(() => {
    if (selectedLga !== 'all' && !lgaOptions.includes(selectedLga)) {
      setSelectedLga('all')
    }
  }, [lgaOptions, selectedLga])

  useEffect(() => {
    if (
      selectedWardKey &&
      !wardOptions.some((feature) => feature.properties.wardKey === selectedWardKey)
    ) {
      setSelectedWardKey('')
    }
  }, [selectedWardKey, wardOptions])

  useEffect(() => {
    if (
      outletAnalysisState !== 'all' &&
      !outletAnalysisStateOptions.includes(outletAnalysisState)
    ) {
      setOutletAnalysisState('all')
    }
  }, [outletAnalysisState, outletAnalysisStateOptions])

  useEffect(() => {
    if (outletAnalysisLga !== 'all' && !outletAnalysisLgaOptions.includes(outletAnalysisLga)) {
      setOutletAnalysisLga('all')
    }
  }, [outletAnalysisLga, outletAnalysisLgaOptions])

  useEffect(() => {
    if (
      outletAnalysisWardKey &&
      !outletAnalysisWardOptions.some(
        (feature) => feature.properties.wardKey === outletAnalysisWardKey,
      )
    ) {
      setOutletAnalysisWardKey('')
    }
  }, [outletAnalysisWardKey, outletAnalysisWardOptions])

  const coverageScopedWards = allWardFeatures.filter((feature) => {
    const matchesState =
      selectedState === 'all' || feature.properties.stateName === selectedState
    const matchesLga = selectedLga === 'all' || feature.properties.lgaName === selectedLga
    const matchesCoverage =
      selectedCoverageStatus === 'all' ||
      feature.properties.coverageStatus === selectedCoverageStatus

    return matchesState && matchesLga && matchesCoverage
  })

  const wardsWithGps = coverageScopedWards.filter(hasWardObservations)
  const scopedWardKeys = new Set(coverageScopedWards.map((feature) => feature.properties.wardKey))
  const analysisScopePoints = allObservationFeatures.filter((feature) =>
    scopedWardKeys.has(feature.properties.wardKey),
  )

  useEffect(() => {
    if (
      selectedWardKey &&
      !coverageScopedWards.some((feature) => feature.properties.wardKey === selectedWardKey)
    ) {
      setSelectedWardKey('')
    }
  }, [coverageScopedWards, selectedWardKey])

  const activeWard = selectedWardKey
    ? dashboard?.wardByKey.get(selectedWardKey) &&
      coverageScopedWards.some((feature) => feature.properties.wardKey === selectedWardKey)
        ? dashboard?.wardByKey.get(selectedWardKey) ?? null
        : null
    : wardsWithGps[0] ?? coverageScopedWards[0] ?? null
  const activeWardAnalysisPoints = useMemo(
    () =>
      activeWard
        ? analysisScopePoints.filter((feature) => feature.properties.wardKey === activeWard.properties.wardKey)
        : [],
    [activeWard, analysisScopePoints],
  )

  const selectedStateFeature =
    selectedState === 'all' ? null : dashboard?.stateByName.get(selectedState) ?? null

  const displayedLgas = (dashboard?.lgas.features ?? []).filter(
    (feature) => selectedState === 'all' || feature.properties.stateName === selectedState,
  )

  const showNationalWardPreview =
    selectedState === 'all' && selectedLga === 'all' && selectedCoverageStatus === 'all'
  const displayedWards = showNationalWardPreview ? wardsWithGps : coverageScopedWards

  const visibleMapPoints = mapPoints.filter((feature) => {
    if (isClusterMapPoint(feature)) {
      return selectedCoverageStatus === 'all'
    }

    return scopedWardKeys.has(feature.properties.wardKey)
  })

  const outOfBoundaryRows = useMemo<OutOfBoundaryRow[]>(() => {
    if (!dashboard) {
      return []
    }

    const rows: OutOfBoundaryRow[] = []

    analysisScopePoints.forEach((feature) => {
      const ward = dashboard.wardByKey.get(feature.properties.wardKey)
      if (!ward) {
        return
      }

      if (geometryContainsPoint(feature.geometry.coordinates, ward.geometry)) {
        return
      }

      const [longitude, latitude] = feature.geometry.coordinates
      const lgaKey = buildAdminKey(feature.properties.stateName, feature.properties.lgaName)
      const stateKey = buildAdminKey(feature.properties.stateName)
      const candidateWards =
        wardsByStateLga.get(lgaKey) ?? wardsByState.get(stateKey) ?? allWardFeatures
      const detectedWard = candidateWards.find((candidate) =>
        geometryContainsPoint(feature.geometry.coordinates, candidate.geometry),
      )

      rows.push({
        id: feature.properties.id,
        businessName: feature.properties.businessName,
        wardKey: feature.properties.wardKey,
        wardCode: feature.properties.wardCode,
        latitude,
        longitude,
        stateName: feature.properties.stateName,
        lgaName: feature.properties.lgaName,
        expectedWardName: feature.properties.wardName,
        detectedWardName: detectedWard?.properties.wardName ?? 'Outside mapped wards',
        detectedLgaName: detectedWard?.properties.lgaName ?? 'No containing ward found',
        collectorName: feature.properties.collectorName,
        deviceId: feature.properties.deviceId,
        status: feature.properties.status,
        statusDetail: feature.properties.statusDetail,
        outletType: feature.properties.outletType,
        channelType: feature.properties.channelType,
        productCategories: feature.properties.productCategories,
        preApproval: feature.properties.preApproval,
        gpsAccuracy: feature.properties.gpsAccuracy,
        gpsQualityFlag: feature.properties.gpsQualityFlag,
        effectiveToleranceM: feature.properties.effectiveToleranceM,
        eventTs: feature.properties.eventTs,
        submissionTs: feature.properties.submissionTs,
        startTime: feature.properties.startTime,
        endTime: feature.properties.endTime,
        surveyDate: feature.properties.surveyDate,
        reviewState: feature.properties.reviewState,
      })
    })

    return rows.sort((first, second) => second.gpsAccuracy - first.gpsAccuracy)
  }, [allWardFeatures, analysisScopePoints, dashboard, wardsByState, wardsByStateLga])

  const duplicateGpsRows = useMemo<DuplicateGpsRow[]>(() => {
    const grouped = new Map<
      string,
      {
        wardKey: string
        latitude: number
        longitude: number
        count: number
        stateName: string
        lgaName: string
        wardName: string
        outletNames: string[]
      }
    >()

    analysisScopePoints.forEach((feature) => {
      const [longitude, latitude] = feature.geometry.coordinates
      const key = `${latitude.toFixed(6)}::${longitude.toFixed(6)}`
      const existing = grouped.get(key)

      if (existing) {
        existing.count += 1
        if (existing.outletNames.length < 3 && !existing.outletNames.includes(feature.properties.businessName)) {
          existing.outletNames.push(feature.properties.businessName)
        }
        return
      }

      grouped.set(key, {
        wardKey: feature.properties.wardKey,
        latitude,
        longitude,
        count: 1,
        stateName: feature.properties.stateName,
        lgaName: feature.properties.lgaName,
        wardName: feature.properties.wardName,
        outletNames: [feature.properties.businessName],
      })
    })

    return Array.from(grouped.entries())
      .filter(([, value]) => value.count > 1)
      .map(([key, value]) => ({
        id: key,
        wardKey: value.wardKey,
        latitude: value.latitude,
        longitude: value.longitude,
        duplicateCount: value.count,
        stateName: value.stateName,
        lgaName: value.lgaName,
        wardName: value.wardName,
        outletNames: value.outletNames.join(', '),
      }))
      .sort((first, second) => second.duplicateCount - first.duplicateCount)
  }, [analysisScopePoints])

  const duplicateGpsCaseRows = useMemo<DuplicateGpsCaseRow[]>(() => {
    const grouped = new Map<
      string,
      {
        duplicateCount: number
        cases: DuplicateGpsCaseRow[]
      }
    >()

    analysisScopePoints.forEach((feature) => {
      const [longitude, latitude] = feature.geometry.coordinates
      const groupId = `${latitude.toFixed(6)}::${longitude.toFixed(6)}`
      const existing = grouped.get(groupId)

      if (existing) {
        existing.cases.push({
          duplicateGroupId: groupId,
          duplicateCount: 0,
          id: feature.properties.id,
          businessName: feature.properties.businessName,
          latitude,
          longitude,
          stateName: feature.properties.stateName,
          lgaName: feature.properties.lgaName,
          wardName: feature.properties.wardName,
          wardCode: feature.properties.wardCode,
          wardKey: feature.properties.wardKey,
          collectorName: feature.properties.collectorName,
          deviceId: feature.properties.deviceId,
          status: feature.properties.status,
          statusDetail: feature.properties.statusDetail,
          outletType: feature.properties.outletType,
          channelType: feature.properties.channelType,
          productCategories: feature.properties.productCategories,
          preApproval: feature.properties.preApproval,
          gpsAccuracy: feature.properties.gpsAccuracy,
          gpsQualityFlag: feature.properties.gpsQualityFlag,
          effectiveToleranceM: feature.properties.effectiveToleranceM,
          eventTs: feature.properties.eventTs,
          submissionTs: feature.properties.submissionTs,
          startTime: feature.properties.startTime,
          endTime: feature.properties.endTime,
          surveyDate: feature.properties.surveyDate,
          reviewState: feature.properties.reviewState,
        })
        return
      }

      grouped.set(groupId, {
        duplicateCount: 0,
        cases: [
          {
            duplicateGroupId: groupId,
            duplicateCount: 0,
            id: feature.properties.id,
            businessName: feature.properties.businessName,
            latitude,
            longitude,
            stateName: feature.properties.stateName,
            lgaName: feature.properties.lgaName,
            wardName: feature.properties.wardName,
            wardCode: feature.properties.wardCode,
            wardKey: feature.properties.wardKey,
            collectorName: feature.properties.collectorName,
            deviceId: feature.properties.deviceId,
            status: feature.properties.status,
            statusDetail: feature.properties.statusDetail,
            outletType: feature.properties.outletType,
            channelType: feature.properties.channelType,
            productCategories: feature.properties.productCategories,
            preApproval: feature.properties.preApproval,
            gpsAccuracy: feature.properties.gpsAccuracy,
            gpsQualityFlag: feature.properties.gpsQualityFlag,
            effectiveToleranceM: feature.properties.effectiveToleranceM,
            eventTs: feature.properties.eventTs,
            submissionTs: feature.properties.submissionTs,
            startTime: feature.properties.startTime,
            endTime: feature.properties.endTime,
            surveyDate: feature.properties.surveyDate,
            reviewState: feature.properties.reviewState,
          },
        ],
      })
    })

    return Array.from(grouped.values())
      .filter((group) => group.cases.length > 1)
      .flatMap((group) =>
        group.cases.map((row) => ({
          ...row,
          duplicateCount: group.cases.length,
        })),
      )
      .sort((first, second) => {
        if (second.duplicateCount !== first.duplicateCount) {
          return second.duplicateCount - first.duplicateCount
        }

        return first.duplicateGroupId.localeCompare(second.duplicateGroupId)
      })
  }, [analysisScopePoints])

  const uncoveredWardRows = useMemo(
    () =>
      coverageScopedWards
        .filter(
          (feature) =>
            feature.properties.coverageStatus === 'under_covered' ||
            feature.properties.coverageStatus === 'no_gps',
        )
        .sort((first, second) => {
          if (first.properties.coverageStatus !== second.properties.coverageStatus) {
            return first.properties.coverageStatus.localeCompare(second.properties.coverageStatus)
          }

          return first.properties.coveragePercent - second.properties.coveragePercent
        }),
    [coverageScopedWards],
  )
  const activeBoundaryDownloadLabel =
    analysisView === 'out_of_boundary'
      ? 'Out of Boundary'
      : analysisView === 'duplicate_gps'
        ? 'Duplicate GPS'
        : 'Uncovered Wards'
  const activeBoundaryRowCount =
    analysisView === 'out_of_boundary'
      ? outOfBoundaryRows.length
      : analysisView === 'duplicate_gps'
        ? duplicateGpsRows.length
        : uncoveredWardRows.length

  useEffect(() => {
    const availableOutletTypes = new Set(outletTypeRows.map((row) => row.outletType))
    const nextSelection = normalizeOutletTypeSelection(
      outletAnalysisOutletTypes.filter(
        (outletType) => outletType === 'all' || availableOutletTypes.has(outletType),
      ),
    )

    const selectionChanged =
      nextSelection.length !== outletAnalysisOutletTypes.length ||
      nextSelection.some((outletType, index) => outletType !== outletAnalysisOutletTypes[index])

    if (selectionChanged) {
      setOutletAnalysisOutletTypes(nextSelection)
    }
  }, [outletAnalysisOutletTypes, outletTypeRows])

  const activeWardKey = activeWard?.properties.wardKey ?? ''

  const stateLayerData = useMemo<FeatureCollection<AreaGeometry, StyledProperties>>(
    () => ({
      type: 'FeatureCollection',
      features: allStateFeatures.map((feature) => {
        const isSelected = selectedState === feature.properties.stateName
        const style = getStatePolygonStyle(
          isSelected,
          feature.properties.hasObservations,
          basemapTone,
        )
        const popupHtml = renderAttributeTooltip(
          feature.properties.stateName,
          [
            { label: 'State code', value: feature.properties.stateCode },
            { label: 'Capital city', value: feature.properties.capitalCity },
            { label: 'Geo zone', value: feature.properties.geoZone },
            { label: 'GPS observations', value: feature.properties.observationCount },
            { label: 'Observed', value: feature.properties.hasObservations },
          ],
          'State boundary',
        )

        return {
          ...feature,
          properties: withStyledProperties(feature.properties, {
            popupHtml,
            ...style,
          }),
        }
      }),
    }),
    [allStateFeatures, basemapTone, selectedState],
  )

  const lgaLayerData = useMemo<FeatureCollection<AreaGeometry, StyledProperties>>(
    () => ({
      type: 'FeatureCollection',
      features: displayedLgas.map((feature) => {
        const style = getLgaLineStyle(feature, selectedState, selectedLga, basemapTone)
        const popupHtml = renderAttributeTooltip(
          feature.properties.lgaName,
          [
            { label: 'State', value: feature.properties.stateName },
            { label: 'LGA code', value: feature.properties.lgaCode },
            { label: 'GPS observations', value: feature.properties.observationCount },
            { label: 'Observed', value: feature.properties.hasObservations },
          ],
          'LGA boundary',
        )

        return {
          ...feature,
          properties: withStyledProperties(feature.properties, {
            popupHtml,
            ...style,
          }),
        }
      }),
    }),
    [basemapTone, displayedLgas, selectedLga, selectedState],
  )

  const wardLayerData = useMemo<FeatureCollection<AreaGeometry, StyledProperties>>(
    () => ({
      type: 'FeatureCollection',
      features: displayedWards.map((feature) => {
        const style = getWardPolygonStyle(feature, activeWardKey, basemapTone)
        const popupHtml = renderAttributeTooltip(
          feature.properties.wardName,
          [
            { label: 'State', value: feature.properties.stateName },
            { label: 'LGA', value: feature.properties.lgaName },
            { label: 'Ward code', value: feature.properties.wardCode },
            { label: 'Coverage status', value: coverageStatusLabels[feature.properties.coverageStatus] },
            { label: 'Coverage', value: formatCoveragePercent(feature.properties.coveragePercent) },
            { label: 'Scored GPS points', value: feature.properties.observationCount },
            { label: 'Raw GPS points', value: feature.properties.rawObservationCount },
            { label: 'Mean GPS accuracy', value: formatMeters(feature.properties.averageAccuracyM) },
            { label: 'Covered area', value: formatArea(feature.properties.coveredAreaM2) },
            { label: 'Uncovered area', value: formatArea(feature.properties.uncoveredAreaM2) },
          ],
          'Ward boundary',
        )

        return {
          ...feature,
          properties: withStyledProperties(feature.properties, {
            popupHtml,
            ...style,
          }),
        }
      }),
    }),
    [activeWardKey, basemapTone, displayedWards],
  )

  const focusBoundaryData = useMemo<FeatureCollection<AreaGeometry, StyledProperties>>(() => {
    const features: FeatureCollection<AreaGeometry, StyledProperties>['features'] = []

    const selectedStateStyledFeature =
      selectedState === 'all'
        ? null
        : stateLayerData.features.find(
            (feature) => feature.properties.stateName === selectedState,
          ) ?? null

    if (selectedStateStyledFeature) {
      features.push({
        ...selectedStateStyledFeature,
        properties: {
          ...selectedStateStyledFeature.properties,
          popupHtml: '',
          lineWidth: Number(selectedStateStyledFeature.properties.lineWidth ?? 3.5) + 0.6,
          lineOpacity: 1,
        },
      })
    }

    const selectedLgaStyledFeature =
      selectedState === 'all' || selectedLga === 'all'
        ? null
        : lgaLayerData.features.find(
            (feature) =>
              feature.properties.stateName === selectedState &&
              feature.properties.lgaName === selectedLga,
          ) ?? null

    if (selectedLgaStyledFeature) {
      features.push({
        ...selectedLgaStyledFeature,
        properties: {
          ...selectedLgaStyledFeature.properties,
          popupHtml: '',
          lineWidth: Number(selectedLgaStyledFeature.properties.lineWidth ?? 3) + 0.6,
          lineOpacity: 1,
        },
      })
    }

    const selectedWardStyledFeature =
      activeWardKey
        ? wardLayerData.features.find(
            (feature) => feature.properties.wardKey === activeWardKey,
          ) ?? null
        : null

    if (selectedWardStyledFeature) {
      features.push({
        ...selectedWardStyledFeature,
        properties: {
          ...selectedWardStyledFeature.properties,
          popupHtml: '',
          lineWidth: Number(selectedWardStyledFeature.properties.lineWidth ?? 4.2) + 0.8,
          lineOpacity: 1,
        },
      })
    }

    return {
      type: 'FeatureCollection',
      features,
    }
  }, [activeWardKey, lgaLayerData.features, selectedLga, selectedState, stateLayerData.features, wardLayerData.features])

  const pointLayerData = useMemo<FeatureCollection<Point, Record<string, unknown>>>(
    () => ({
      type: 'FeatureCollection',
      features: visibleMapPoints.map((feature) => {
        if (isClusterMapPoint(feature)) {
          return {
            ...feature,
            properties: {
              ...feature.properties,
              popupHtml: '',
            },
          }
        }

        const style = getObservationCircleStyle(feature, activeWardKey)
        const [longitude, latitude] = feature.geometry.coordinates
        const popupHtml = renderAttributeTooltip(
          feature.properties.businessName,
          [
            { label: 'Status', value: feature.properties.status },
            { label: 'Status detail', value: feature.properties.statusDetail },
            { label: 'Collector', value: feature.properties.collectorName },
            { label: 'Device ID', value: feature.properties.deviceId },
            { label: 'State', value: feature.properties.stateName },
            { label: 'LGA', value: feature.properties.lgaName },
            { label: 'Ward', value: feature.properties.wardName },
            { label: 'Ward code', value: feature.properties.wardCode },
            { label: 'Outlet type', value: feature.properties.outletType },
            { label: 'Product categories', value: feature.properties.productCategories },
            { label: 'Channel type', value: feature.properties.channelType },
            { label: 'GPS accuracy', value: formatMeters(feature.properties.gpsAccuracy) },
            { label: 'GPS quality', value: feature.properties.gpsQualityFlag },
            { label: 'Scoring tolerance', value: formatMeters(feature.properties.effectiveToleranceM) },
            { label: 'Event time', value: feature.properties.eventTs },
            { label: 'Survey date', value: feature.properties.surveyDate },
            { label: 'Review state', value: feature.properties.reviewState },
            { label: 'Latitude', value: latitude.toFixed(6) },
            { label: 'Longitude', value: longitude.toFixed(6) },
          ],
          'GPS observation',
        )

        return {
          ...feature,
          properties: withStyledProperties(feature.properties, {
            popupHtml,
            ...style,
          }),
        }
      }),
    }),
    [activeWardKey, visibleMapPoints],
  )

  const haloLayerData = useMemo<FeatureCollection<Polygon, StyledProperties>>(
    () => ({
      type: 'FeatureCollection',
      features: [],
    }),
    [],
  )

  const selectedPointLayerData = useMemo<FeatureCollection<Point>>(
    () => ({
      type: 'FeatureCollection',
      features: selectedPoint
        ? [
            {
              type: 'Feature',
              id: selectedPoint.id,
              geometry: {
                type: 'Point',
                coordinates: [selectedPoint.longitude, selectedPoint.latitude],
              },
              properties: {},
            },
          ]
        : [],
    }),
    [selectedPoint],
  )

  const hasScopedViewport =
    selectedState !== 'all' || selectedLga !== 'all' || selectedCoverageStatus !== 'all'
  const viewportBounds =
    focusMode === 'ward' && activeWard
      ? getFeatureBounds(activeWard)
      : hasScopedViewport
        ? getCollectionBounds(
            displayedWards.length > 0
              ? displayedWards
              : selectedStateFeature
                ? [selectedStateFeature]
                : allStateFeatures,
          )
        : getCollectionBounds(allStateFeatures)
  const viewportMaxZoom = focusMode === 'ward' ? 14 : hasScopedViewport ? 10 : 7
  const pointTileUrl = dashboard
    ? buildPointTileUrl({
        datasetId: dashboard.activeDataset.id,
        stateName: selectedState,
        lgaName: selectedLga,
        coverageStatus: selectedCoverageStatus,
      })
    : ''

  useEffect(() => {
    if (!dashboard || focusMode === 'overview' || !activeWard) {
      setMapPoints([])
      setIsMapPointsLoading(false)
      return
    }

    if (activeWardAnalysisPoints.length > 0) {
      startTransition(() => {
        setMapPoints(activeWardAnalysisPoints)
        setIsMapPointsLoading(false)
      })
      return
    }

    if (activeWard.properties.rawObservationCount <= 0) {
      setMapPoints([])
      setIsMapPointsLoading(false)
      return
    }

    const detailBounds = getFeatureBounds(activeWard)
    if (!detailBounds) {
      setMapPoints([])
      setIsMapPointsLoading(false)
      return
    }

    let ignore = false
    const timeoutId = window.setTimeout(() => {
      setIsMapPointsLoading(true)
      loadMapPoints({
        datasetId: dashboard.activeDataset.id,
        bbox: [
          detailBounds[0][0],
          detailBounds[0][1],
          detailBounds[1][0],
          detailBounds[1][1],
        ],
        zoom: 16,
        stateName: activeWard.properties.stateName,
        lgaName: activeWard.properties.lgaName,
      })
        .then((collection) => {
          if (ignore) {
            return
          }

          startTransition(() => {
            setMapPoints(collection.features)
            setIsMapPointsLoading(false)
          })
        })
        .catch(() => {
          if (ignore) {
            return
          }

          setIsMapPointsLoading(false)
        })
    }, 160)

    return () => {
      ignore = true
      window.clearTimeout(timeoutId)
    }
  }, [activeWard, activeWardAnalysisPoints, dashboard, focusMode])

  if (showStartupLoading && !loadError) {
    return (
      <main className="dashboard-shell dashboard-shell--loading">
        <section className="loading-panel">
          <img className="loading-panel__logo" src="/infinity-logo.png" alt="Infinity logo" />
          <span className="panel-eyebrow">Initializing map data</span>
          <h1>Building the Nigeria Census workspace.</h1>
          <p>Loading dashboard metrics and map boundaries. GPS tiles and analysis data stream in on demand for speed.</p>
          <div className="loading-progress" aria-live="polite" aria-label="Loading progress">
            <div className="loading-progress__meta">
              <strong>{loadingProgress}%</strong>
              <span>{loadingPhaseLabel}</span>
            </div>
            <div className="loading-progress__track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={loadingProgress}>
              <div
                className="loading-progress__fill"
                style={{ width: `${loadingProgress}%` }}
              />
            </div>
          </div>
        </section>
      </main>
    )
  }

  if (!dashboard) {
    return (
      <main className="dashboard-shell dashboard-shell--loading">
        <section className="loading-panel loading-panel--error">
          <img className="loading-panel__logo" src="/infinity-logo.png" alt="Infinity logo" />
          <span className="panel-eyebrow">Load failure</span>
          <h1>Dashboard data could not be loaded.</h1>
          <p>{loadError ?? 'The startup loader completed, but the dashboard payload is still unavailable.'}</p>
        </section>
      </main>
    )
  }

  const normalizedSyncTimestamp = dashboard.summary.generatedAt
    ? dashboard.summary.generatedAt
        .replace(' ', 'T')
        .replace(/([+-]\d{2})(\d{2})$/, '$1:$2')
        .replace(/([+-]\d{2})$/, '$1:00')
    : ''
  const lastSyncedAt = new Date(normalizedSyncTimestamp)
  const hasValidSyncTimestamp = !Number.isNaN(lastSyncedAt.getTime())
  const lastSyncedDate = hasValidSyncTimestamp
    ? new Intl.DateTimeFormat('en-NG', {
        dateStyle: 'long',
        timeZone: 'Africa/Lagos',
      }).format(lastSyncedAt)
    : 'N/A'
  const lastSyncedTime = hasValidSyncTimestamp
    ? new Intl.DateTimeFormat('en-NG', {
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit',
        timeZone: 'Africa/Lagos',
        timeZoneName: 'short',
      }).format(lastSyncedAt)
    : 'N/A'
  const activeDatasetId = selectedDatasetId || dashboard.activeDataset.id
  const targetDatasetLabel =
    dashboard.datasetOptions.find((dataset) => dataset.id === activeDatasetId)?.label ??
    activeDatasetId
  const resetDatasetViewState = () => {
    setSelectedState('all')
    setSelectedLga('all')
    setSelectedCoverageStatus('all')
    setSelectedWardKey('')
    setFocusMode('overview')
    setAnalysisView('out_of_boundary')
    setSelectedAnalysisRowId('')
    setSelectedPoint(null)
    setOutletAnalysisState('all')
    setOutletAnalysisLga('all')
    setOutletAnalysisWardKey('')
    setOutletAnalysisView('table')
    setOutletAnalysisOutletTypes(['all'])
  }

  const clearFilters = () => {
    startTransition(() => {
      setSelectedState('all')
      setSelectedLga('all')
      setSelectedCoverageStatus('all')
      setSelectedWardKey('')
      setSelectedPoint(null)
      setSelectedAnalysisRowId('')
      setFocusMode('overview')
    })
  }

  const handleDatasetSelection = (datasetId: string) => {
    startTransition(() => {
      setSelectedDatasetId(datasetId)
      resetDatasetViewState()
    })
  }

  const handleStateSelection = (stateName: string) => {
    startTransition(() => {
      setSelectedState(stateName)
      setSelectedLga('all')
      setSelectedWardKey('')
      setSelectedPoint(null)
      setSelectedAnalysisRowId('')
      setFocusMode('overview')
    })
  }

  const handleWardSelection = (wardKey: string) => {
    startTransition(() => {
      setSelectedWardKey(wardKey)
      setSelectedPoint(null)
      setSelectedAnalysisRowId('')
      setFocusMode('ward')
    })
  }

  const handleLgaSelection = (stateName: string, lgaName: string) => {
    startTransition(() => {
      setSelectedState(stateName)
      setSelectedLga(lgaName)
      setSelectedWardKey('')
      setSelectedPoint(null)
      setSelectedAnalysisRowId('')
      setFocusMode('overview')
    })
  }

  const handleAnalysisPointSelection = (
    rowId: string,
    wardKey: string,
    latitude: number,
    longitude: number,
  ) => {
    startTransition(() => {
      setSelectedWardKey(wardKey)
      setSelectedPoint({
        id: rowId,
        latitude,
        longitude,
      })
      setSelectedAnalysisRowId(rowId)
      setFocusMode('point')
    })
  }

  const handleAnalysisWardSelection = (wardKey: string) => {
    startTransition(() => {
      setSelectedWardKey(wardKey)
      setSelectedPoint(null)
      setSelectedAnalysisRowId(wardKey)
      setFocusMode('ward')
    })
  }

  const handleOutletTypeToggle = (outletType: string) => {
    startTransition(() => {
      setOutletAnalysisOutletTypes((currentSelection) => {
        if (outletType === 'all') {
          return ['all']
        }

        const baseSelection = currentSelection.filter((value) => value !== 'all')
        const nextSelection = baseSelection.includes(outletType)
          ? baseSelection.filter((value) => value !== outletType)
          : [...baseSelection, outletType]

        return normalizeOutletTypeSelection(nextSelection)
      })
    })
  }

  const handleDownloadBoundaryChecks = () => {
    if (!dashboard) {
      return
    }

    const datasetId = dashboard.activeDataset.id || 'dataset'
    const filename = `${slugifyFilePart(datasetId)}-${slugifyFilePart(activeBoundaryDownloadLabel)}.csv`

    if (analysisView === 'out_of_boundary') {
      downloadCsvFile(
        filename,
        [
          'Case Key',
          'Business',
          'Latitude',
          'Longitude',
          'State',
          'Expected LGA',
          'Expected Ward',
          'Expected Ward Key',
          'Expected Ward Code',
          'Detected LGA',
          'Detected Ward',
          'Collector',
          'Device ID',
          'Status',
          'Status detail',
          'Outlet type',
          'Channel type',
          'Product categories',
          'Pre-approval',
          'Accuracy (m)',
          'GPS quality flag',
          'Effective tolerance (m)',
          'Event timestamp',
          'Submission timestamp',
          'Start time',
          'End time',
          'Survey date',
          'Review state',
        ],
        outOfBoundaryRows.map((row) => [
          row.id,
          row.businessName,
          row.latitude.toFixed(6),
          row.longitude.toFixed(6),
          row.stateName,
          row.lgaName,
          row.expectedWardName,
          row.wardKey,
          row.wardCode,
          row.detectedLgaName,
          row.detectedWardName,
          row.collectorName,
          row.deviceId,
          row.status,
          row.statusDetail,
          row.outletType,
          row.channelType,
          row.productCategories.join(', '),
          row.preApproval ? 'Yes' : 'No',
          decimalFormatter.format(row.gpsAccuracy),
          row.gpsQualityFlag,
          decimalFormatter.format(row.effectiveToleranceM),
          row.eventTs ?? '',
          row.submissionTs ?? '',
          row.startTime ?? '',
          row.endTime ?? '',
          row.surveyDate ?? '',
          row.reviewState,
        ]),
      )
      return
    }

    if (analysisView === 'duplicate_gps') {
      downloadCsvFile(
        filename,
        [
          'Duplicate group',
          'Duplicate count',
          'Case key',
          'Business',
          'Latitude',
          'Longitude',
          'State',
          'LGA',
          'Ward',
          'Ward key',
          'Ward code',
          'Collector',
          'Device ID',
          'Status',
          'Status detail',
          'Outlet type',
          'Channel type',
          'Product categories',
          'Pre-approval',
          'Accuracy (m)',
          'GPS quality flag',
          'Effective tolerance (m)',
          'Event timestamp',
          'Submission timestamp',
          'Start time',
          'End time',
          'Survey date',
          'Review state',
        ],
        duplicateGpsCaseRows.map((row) => [
          row.duplicateGroupId,
          row.duplicateCount,
          row.id,
          row.businessName,
          row.latitude.toFixed(6),
          row.longitude.toFixed(6),
          row.stateName,
          row.lgaName,
          row.wardName,
          row.wardKey,
          row.wardCode,
          row.collectorName,
          row.deviceId,
          row.status,
          row.statusDetail,
          row.outletType,
          row.channelType,
          row.productCategories.join(', '),
          row.preApproval ? 'Yes' : 'No',
          decimalFormatter.format(row.gpsAccuracy),
          row.gpsQualityFlag,
          decimalFormatter.format(row.effectiveToleranceM),
          row.eventTs ?? '',
          row.submissionTs ?? '',
          row.startTime ?? '',
          row.endTime ?? '',
          row.surveyDate ?? '',
          row.reviewState,
        ]),
      )
      return
    }

    downloadCsvFile(
      filename,
      [
        'Ward',
        'Ward key',
        'Ward code',
        'State',
        'LGA',
        'Status',
        'Coverage',
        'Scored GPS points',
        'Raw GPS points',
        'Covered cells',
        'Total cells',
        'Covered area (m2)',
        'Uncovered area (m2)',
        'Average accuracy (m)',
        'Urban class',
      ],
      uncoveredWardRows.map((feature) => [
        feature.properties.wardName,
        feature.properties.wardKey,
        feature.properties.wardCode,
        feature.properties.stateName,
        feature.properties.lgaName,
        coverageStatusLabels[feature.properties.coverageStatus],
        formatCoveragePercent(feature.properties.coveragePercent),
        feature.properties.observationCount,
        feature.properties.rawObservationCount,
        feature.properties.coveredCells,
        feature.properties.totalCells,
        decimalFormatter.format(feature.properties.coveredAreaM2),
        decimalFormatter.format(feature.properties.uncoveredAreaM2),
        feature.properties.averageAccuracyM == null
          ? ''
          : decimalFormatter.format(feature.properties.averageAccuracyM),
        feature.properties.urbanClass ?? '',
      ]),
    )
  }

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div className="brand-cluster">
          <img className="brand-logo" src="/infinity-logo.png" alt="Infinity logo" />
          <div className="brand-copy">
            <strong>Nigeria Census Coverage</strong>
            <span>{dashboard.activeDataset.sourceFile}</span>
          </div>
        </div>
        <div className="topbar__meta">
          <label className="field field--topbar">
            <span>Census Dataset</span>
            <select
              value={activeDatasetId}
              onChange={(event) => handleDatasetSelection(event.target.value)}
              disabled={dashboard.datasetOptions.length <= 1}
            >
              {dashboard.datasetOptions.map((dataset) => (
                <option key={dataset.id} value={dataset.id}>
                  {dataset.label}
                </option>
              ))}
            </select>
          </label>
          <div className="topbar__sync">
            <div className="sync-meta">
              <span className="panel-eyebrow">Last Synced Date</span>
              <strong>{lastSyncedDate}</strong>
            </div>
            <div className="sync-meta">
              <span className="panel-eyebrow">Last Synced Time</span>
              <strong>{lastSyncedTime}</strong>
            </div>
          </div>
        </div>
      </header>

      {isDatasetLoading ? (
        <section className="dataset-status dataset-status--loading">
          Loading dataset: <strong>{targetDatasetLabel}</strong>
        </section>
      ) : null}

      {!isDatasetLoading && isMapPointsLoading ? (
        <section className="dataset-status dataset-status--loading">
          Refreshing map points for the current viewport.
        </section>
      ) : null}

      {!isDatasetLoading && (isAnalysisLoading || isOutletAnalysisLoading) ? (
        <section className="dataset-status dataset-status--loading">
          Loading analysis tables in the background.
        </section>
      ) : null}

      {loadError ? (
        <section className="dataset-status dataset-status--error">{loadError}</section>
      ) : null}

      <section className="workspace-grid">
        <aside className="control-rail">
          <div className="control-rail__content">
            <div className="control-rail__header">
              <div>
                <h2>Filter</h2>
              </div>
            </div>

            <div className="filter-section">
              <div className="filter-section__title">Coverage scope</div>

              <SelectField label="State" value={selectedState} onChange={handleStateSelection}>
                <option value="all">All states</option>
                {stateOptions.map((stateName) => (
                  <option key={stateName} value={stateName}>
                    {stateName}
                  </option>
                ))}
              </SelectField>

              <SelectField
                label="LGA"
                value={selectedLga}
                onChange={(value) => {
                  startTransition(() => {
                    setSelectedLga(value)
                    setSelectedWardKey('')
                    setSelectedPoint(null)
                    setSelectedAnalysisRowId('')
                    setFocusMode('overview')
                  })
                }}
              >
                <option value="all">All LGAs</option>
                {lgaOptions.map((lgaName) => (
                  <option key={lgaName} value={lgaName}>
                    {lgaName}
                  </option>
                ))}
              </SelectField>

              <SelectField
                label="Ward"
                value={selectedWardKey}
                onChange={(value) => {
                  startTransition(() => {
                    setSelectedWardKey(value)
                    setSelectedPoint(null)
                    setSelectedAnalysisRowId('')
                    setFocusMode(value ? 'ward' : 'overview')
                  })
                }}
              >
                <option value="">All wards in scope</option>
                {wardOptions.map((feature) => (
                  <option key={feature.properties.wardKey} value={feature.properties.wardKey}>
                    {formatWardFilterLabel(feature, selectedState, selectedLga)}
                  </option>
                ))}
              </SelectField>
            </div>

            <div className="filter-section">
              <div className="filter-section__title">Active filters</div>
              <ul className="filter-summary-list">
                <li>{formatFilterValue(selectedState, 'All states')}</li>
                <li>{formatFilterValue(selectedLga, 'All LGAs')}</li>
                <li>{selectedWardKey ? '1 ward selected' : 'All wards in scope'}</li>
              </ul>
            </div>

            <button className="clear-button" type="button" onClick={clearFilters}>
              Reset filters
            </button>
          </div>
        </aside>

        <section className="workspace-panel">
          <section className="metric-strip">
            <MetricCard
              label="Total Achieved"
              value={numberFormatter.format(dashboard.summary.totalAchieved)}
            />
            <MetricCard
              label="Number of Completed"
              value={numberFormatter.format(dashboard.summary.completedCount)}
            />
            <MetricCard
              label="Number of Observation"
              value={numberFormatter.format(dashboard.summary.observationCount)}
            />
            <MetricCard
              label="Number of Wards Visited"
              value={numberFormatter.format(dashboard.summary.wardsVisitedCount)}
            />
            <MetricCard
              label="Number of LGAs Visited"
              value={numberFormatter.format(dashboard.summary.lgasVisitedCount)}
            />
          </section>

          <section className="map-stage">
            <div className="map-stage__controls">
              <div className="map-stage__controls-copy">
                <span className="panel-eyebrow">Basemap</span>
                <strong>Switch the background map</strong>
              </div>

              <div className="basemap-switcher" role="group" aria-label="Basemap switcher">
                {basemapOptions.map((option) => (
                  <button
                    key={option.id}
                    className={`basemap-option ${selectedBasemap === option.id ? 'is-active' : ''}`}
                    type="button"
                    onClick={() => setSelectedBasemap(option.id)}
                  >
                    <strong>{option.label}</strong>
                    <span>{option.description}</span>
                  </button>
                ))}
              </div>
            </div>

            <div className="map-frame">
              <CoverageMap
                basemap={activeBasemap}
                pointTileUrl={focusMode === 'overview' ? pointTileUrl : ''}
                stateData={stateLayerData}
                lgaData={lgaLayerData}
                wardData={wardLayerData}
                focusBoundaryData={focusBoundaryData}
                haloData={haloLayerData}
                pointData={pointLayerData}
                selectedPointData={selectedPointLayerData}
                viewportBounds={viewportBounds}
                viewportMaxZoom={viewportMaxZoom}
                selectedPoint={selectedPoint}
                onStateSelect={handleStateSelection}
                onLgaSelect={handleLgaSelection}
                onWardSelect={handleWardSelection}
                onViewportChange={() => {}}
              />
            </div>
          </section>

          <section ref={analysisStageRef} className="analysis-stage">
            <div className="analysis-stage__header">
              <div>
                <span className="panel-eyebrow">Analysis table</span>
                <h2>Boundary and quality checks</h2>
              </div>
              <button
                className="analysis-download-button"
                type="button"
                onClick={handleDownloadBoundaryChecks}
                disabled={activeBoundaryRowCount === 0}
              >
                Download {activeBoundaryDownloadLabel}
              </button>
            </div>

            <div className="analysis-tabs">
              <button
                className={
                  analysisView === 'out_of_boundary'
                    ? 'analysis-tab is-active'
                    : 'analysis-tab'
                }
                type="button"
                onClick={() => setAnalysisView('out_of_boundary')}
              >
                Out of Boundary
              </button>
              <button
                className={
                  analysisView === 'duplicate_gps' ? 'analysis-tab is-active' : 'analysis-tab'
                }
                type="button"
                onClick={() => setAnalysisView('duplicate_gps')}
              >
                Duplicate GPS
              </button>
              <button
                className={
                  analysisView === 'uncovered_wards'
                    ? 'analysis-tab is-active'
                    : 'analysis-tab'
                }
                type="button"
                onClick={() => setAnalysisView('uncovered_wards')}
              >
                Uncovered Wards
              </button>
            </div>

            <div className="analysis-table-shell">
              {analysisView === 'out_of_boundary' ? (
                outOfBoundaryRows.length === 0 ? (
                  <div className="analysis-empty">No out-of-boundary GPS points in the current scope.</div>
                ) : (
                  <table className="analysis-table">
                    <thead>
                      <tr>
                        <th>Case Key</th>
                        <th>Business</th>
                        <th>Latitude</th>
                        <th>Longitude</th>
                        <th>State</th>
                        <th>Expected LGA</th>
                        <th>Expected Ward</th>
                        <th>Detected LGA</th>
                        <th>Detected Ward</th>
                        <th>Collector</th>
                        <th>Status</th>
                        <th>Accuracy (m)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {outOfBoundaryRows.map((row) => (
                        <tr
                          key={row.id}
                          className={
                            selectedAnalysisRowId === row.id
                              ? 'analysis-table__row is-active'
                              : 'analysis-table__row'
                          }
                          onClick={() =>
                            handleAnalysisPointSelection(
                              row.id,
                              row.wardKey,
                              row.latitude,
                              row.longitude,
                            )
                          }
                        >
                          <td>{row.id}</td>
                          <td>{row.businessName}</td>
                          <td>{row.latitude.toFixed(6)}</td>
                          <td>{row.longitude.toFixed(6)}</td>
                          <td>{row.stateName}</td>
                          <td>{row.lgaName}</td>
                          <td>{row.expectedWardName}</td>
                          <td>{row.detectedLgaName}</td>
                          <td>{row.detectedWardName}</td>
                          <td>{row.collectorName}</td>
                          <td>{row.status}</td>
                          <td>{decimalFormatter.format(row.gpsAccuracy)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              ) : analysisView === 'duplicate_gps' ? (
                duplicateGpsRows.length === 0 ? (
                  <div className="analysis-empty">No duplicate GPS coordinates in the current scope.</div>
                ) : (
                  <table className="analysis-table">
                    <thead>
                      <tr>
                        <th>Latitude</th>
                        <th>Longitude</th>
                        <th>Duplicate count</th>
                        <th>State</th>
                        <th>LGA</th>
                        <th>Ward</th>
                        <th>Sample outlets</th>
                      </tr>
                    </thead>
                    <tbody>
                      {duplicateGpsRows.map((row) => (
                        <tr
                          key={row.id}
                          className={
                            selectedAnalysisRowId === row.id
                              ? 'analysis-table__row is-active'
                              : 'analysis-table__row'
                          }
                          onClick={() =>
                            handleAnalysisPointSelection(
                              row.id,
                              row.wardKey,
                              row.latitude,
                              row.longitude,
                            )
                          }
                        >
                          <td>{row.latitude.toFixed(6)}</td>
                          <td>{row.longitude.toFixed(6)}</td>
                          <td>{numberFormatter.format(row.duplicateCount)}</td>
                          <td>{row.stateName}</td>
                          <td>{row.lgaName}</td>
                          <td>{row.wardName}</td>
                          <td>{row.outletNames}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              ) : analysisView === 'uncovered_wards' ? (
                uncoveredWardRows.length === 0 ? (
                  <div className="analysis-empty">No uncovered wards in the current scope.</div>
                ) : (
                  <table className="analysis-table">
                    <thead>
                      <tr>
                        <th>Ward</th>
                        <th>State</th>
                        <th>LGA</th>
                        <th>Status</th>
                        <th>Coverage</th>
                        <th>Scored GPS points</th>
                      </tr>
                    </thead>
                    <tbody>
                      {uncoveredWardRows.map((feature) => (
                        <tr
                          key={feature.properties.wardKey}
                          className={
                            selectedAnalysisRowId === feature.properties.wardKey
                              ? 'analysis-table__row is-active'
                              : 'analysis-table__row'
                          }
                          onClick={() => handleAnalysisWardSelection(feature.properties.wardKey)}
                        >
                          <td>{feature.properties.wardName}</td>
                          <td>{feature.properties.stateName}</td>
                          <td>{feature.properties.lgaName}</td>
                          <td>{coverageStatusLabels[feature.properties.coverageStatus]}</td>
                          <td>{formatCoveragePercent(feature.properties.coveragePercent)}</td>
                          <td>{numberFormatter.format(feature.properties.observationCount)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              ) : null}
            </div>
          </section>
        </section>
      </section>

      <section ref={outletStageRef} className="outlet-stage">
        <div className="outlet-stage__sticky">
          <div className="outlet-stage__header">
            <div className="outlet-stage__header-copy">
              <span className="panel-eyebrow">Outlet Type Analysis</span>
              <h2>{outletAnalysisTitle}</h2>
            </div>
            <div
              className="outlet-stage__granularity-switch"
              role="group"
              aria-label="Outlet analysis granularity"
            >
                <button
                  className={
                    outletAnalysisGranularity === 'category'
                      ? 'analysis-tab is-active'
                      : 'analysis-tab'
                  }
                  type="button"
                  onClick={() => setOutletAnalysisGranularity('category')}
                >
                  Category
                </button>
                <button
                  className={
                    outletAnalysisGranularity === 'subcategory'
                      ? 'analysis-tab is-active'
                      : 'analysis-tab'
                  }
                  type="button"
                  onClick={() => setOutletAnalysisGranularity('subcategory')}
                >
                  Sub category
                </button>
            </div>
            <div
              className="outlet-stage__display-switch"
              role="group"
              aria-label="Outlet analysis view"
            >
                <button
                  className={
                    outletAnalysisView === 'table'
                      ? 'analysis-tab is-active'
                      : 'analysis-tab'
                  }
                  type="button"
                  onClick={() => setOutletAnalysisView('table')}
                >
                  Table
                </button>
                <button
                  className={
                    outletAnalysisView === 'chart'
                      ? 'analysis-tab is-active'
                      : 'analysis-tab'
                  }
                  type="button"
                  onClick={() => setOutletAnalysisView('chart')}
                >
                  Chart
                </button>
            </div>
          </div>

          <div className="outlet-stage__filters">
            <SelectField
              label="State"
              value={outletAnalysisState}
              onChange={(value) => {
                startTransition(() => {
                  setOutletAnalysisState(value)
                  setOutletAnalysisLga('all')
                  setOutletAnalysisWardKey('')
                  setOutletAnalysisOutletTypes(['all'])
                })
              }}
            >
              <option value="all">All states</option>
              {outletAnalysisStateOptions.map((stateName) => (
                <option key={stateName} value={stateName}>
                  {stateName}
                </option>
              ))}
            </SelectField>

            <SelectField
              label="LGA"
              value={outletAnalysisLga}
              onChange={(value) => {
                startTransition(() => {
                  setOutletAnalysisLga(value)
                  setOutletAnalysisWardKey('')
                  setOutletAnalysisOutletTypes(['all'])
                })
              }}
            >
              <option value="all">All LGAs</option>
              {outletAnalysisLgaOptions.map((lgaName) => (
                <option key={lgaName} value={lgaName}>
                  {lgaName}
                </option>
              ))}
            </SelectField>

            <SelectField
              label="Ward"
              value={outletAnalysisWardKey}
              onChange={(value) => {
                startTransition(() => {
                  setOutletAnalysisWardKey(value)
                  setOutletAnalysisOutletTypes(['all'])
                })
              }}
            >
              <option value="">All wards</option>
              {outletAnalysisWardOptions.map((feature) => (
                <option key={feature.properties.wardKey} value={feature.properties.wardKey}>
                  {outletAnalysisLga !== 'all'
                    ? feature.properties.wardName
                    : outletAnalysisState !== 'all'
                      ? `${feature.properties.wardName} · ${feature.properties.lgaName}`
                      : `${feature.properties.wardName} · ${feature.properties.lgaName}, ${feature.properties.stateName}`}
                </option>
              ))}
            </SelectField>

            <button
              className="clear-button outlet-stage__reset"
              type="button"
              onClick={() => {
                startTransition(() => {
                  setOutletAnalysisState('all')
                  setOutletAnalysisLga('all')
                  setOutletAnalysisWardKey('')
                  setOutletAnalysisOutletTypes(['all'])
                  setOutletAnalysisView('table')
                })
              }}
            >
              Reset outlet filters
            </button>
          </div>

          <div className="outlet-type-filter-bar" role="group" aria-label="Outlet type filters">
            <button
              className={
                outletAnalysisOutletTypes.length === 1 && outletAnalysisOutletTypes[0] === 'all'
                  ? 'outlet-type-filter is-active'
                  : 'outlet-type-filter'
              }
              type="button"
              onClick={() => handleOutletTypeToggle('all')}
            >
              <strong>All outlet types</strong>
              <span>{numberFormatter.format(outletAnalysisScopeRecordCount)} records</span>
            </button>
            {outletTypeRows.map((row) => (
              <button
                key={row.outletType}
                className={
                  outletAnalysisOutletTypes.includes(row.outletType)
                    ? 'outlet-type-filter is-active'
                    : 'outlet-type-filter'
                }
                type="button"
                onClick={() => handleOutletTypeToggle(row.outletType)}
              >
                <strong>{row.outletType}</strong>
                <span>{numberFormatter.format(row.count)} records</span>
              </button>
            ))}
          </div>
          <div className="outlet-stage__summary">
            <strong>{outletAnalysisActiveOutletTypeLabel}</strong>
            <span>{numberFormatter.format(outletAnalysisFilteredRecordCount)} records in scope</span>
          </div>
        </div>

        <div className="analysis-table-shell">
          {!hasOutletAnalysisRows ? (
            <div className="analysis-empty">{outletAnalysisEmptyMessage}</div>
          ) : (
            outletAnalysisView === 'table' ? (
              <div className="analysis-table-scroll">
                {outletAnalysisTableContent}
              </div>
            ) : (
              <>
                <div className="outlet-chart">{outletAnalysisChartContent}</div>
                {/* Legacy category-only chart branch retained intentionally during refactor.
                  <div key={row.categoryName} className="outlet-chart__row">
                    <div className="outlet-chart__meta">
                      <strong>{row.categoryName}</strong>
                      <span>
                        {numberFormatter.format(row.count)} records · {decimalFormatter.format(row.sharePercent)}%
                      </span>
                    </div>
                    <div className="outlet-chart__track">
                      <div
                        className="outlet-chart__fill"
                        style={{ width: `${Math.min(100, row.sharePercent)}%` }}
                      />
                    </div>
                  </div>
                */}
              </>
            )
          )}
        </div>
      </section>
    </main>
  )
}
