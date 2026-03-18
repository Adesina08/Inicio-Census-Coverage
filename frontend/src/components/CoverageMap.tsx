import { useEffect, useRef, useState, type MutableRefObject } from 'react'
import type { FeatureCollection } from 'geojson'
import maplibregl, {
  type GeoJSONSource,
  type LngLat,
  type LngLatBoundsLike,
  type MapGeoJSONFeature,
  NavigationControl,
  Popup,
  type StyleSpecification,
} from 'maplibre-gl'
import type { MapBounds } from '../lib/coverage'

type BasemapLayerDefinition = {
  attribution: string
  url: string
}

type BasemapDefinition = {
  id: string
  backgroundColor?: string
  layers: BasemapLayerDefinition[]
}

type FocusPoint = {
  id: string
  latitude: number
  longitude: number
}

type CoverageMapProps = {
  basemap: BasemapDefinition
  pointTileUrl: string
  stateData: FeatureCollection
  lgaData: FeatureCollection
  wardData: FeatureCollection
  focusBoundaryData: FeatureCollection
  haloData: FeatureCollection
  pointData: FeatureCollection
  selectedPointData: FeatureCollection
  viewportBounds: MapBounds | null
  viewportMaxZoom: number
  selectedPoint: FocusPoint | null
  onStateSelect: (stateName: string) => void
  onLgaSelect: (stateName: string, lgaName: string) => void
  onWardSelect: (wardKey: string) => void
  onViewportChange: (bounds: MapBounds, zoom: number) => void
}

const EMPTY_STYLE: StyleSpecification = {
  version: 8,
  sources: {},
  layers: [
    {
      id: 'background',
      type: 'background',
      paint: {
        'background-color': '#eef2f7',
      },
    },
  ],
}

const SOURCE_IDS = {
  states: 'states-source',
  lgas: 'lgas-source',
  wards: 'wards-source',
  focusBoundaries: 'focus-boundaries-source',
  halos: 'halos-source',
  pointTiles: 'point-tiles-source',
  points: 'points-source',
  selectedPoint: 'selected-point-source',
} as const

const LAYER_IDS = {
  statesFill: 'states-fill',
  statesLine: 'states-line',
  lgasLine: 'lgas-line',
  wardsFill: 'wards-fill',
  wardsLine: 'wards-line',
  focusBoundariesLine: 'focus-boundaries-line',
  halosFill: 'halos-fill',
  halosLine: 'halos-line',
  pointTilesCircle: 'point-tiles-circle',
  pointsCircle: 'points-circle',
  selectedOuter: 'selected-point-outer',
  selectedInner: 'selected-point-inner',
} as const

const OVERLAY_LAYER_ORDER = [
  LAYER_IDS.statesFill,
  LAYER_IDS.statesLine,
  LAYER_IDS.lgasLine,
  LAYER_IDS.wardsFill,
  LAYER_IDS.wardsLine,
  LAYER_IDS.halosFill,
  LAYER_IDS.halosLine,
  LAYER_IDS.focusBoundariesLine,
  LAYER_IDS.pointTilesCircle,
  LAYER_IDS.pointsCircle,
  LAYER_IDS.selectedOuter,
  LAYER_IDS.selectedInner,
] as const

const BASEMAP_SOURCE_PREFIX = 'basemap-source-'
const BASEMAP_LAYER_PREFIX = 'basemap-layer-'
const POINT_TILE_LAYER_NAME = 'points'

function numbersAreClose(first: number, second: number, epsilon = 1e-6) {
  return Math.abs(first - second) <= epsilon
}

function boundsAreEqual(first: MapBounds, second: MapBounds, epsilon = 1e-6) {
  return (
    numbersAreClose(first[0][0], second[0][0], epsilon) &&
    numbersAreClose(first[0][1], second[0][1], epsilon) &&
    numbersAreClose(first[1][0], second[1][0], epsilon) &&
    numbersAreClose(first[1][1], second[1][1], epsilon)
  )
}

function createEmptyCollection(): FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [],
  }
}

function ensureGeoJsonSource(map: maplibregl.Map, id: string, data: FeatureCollection) {
  const existing = map.getSource(id) as GeoJSONSource | undefined

  if (existing) {
    existing.setData(data)
    return
  }

  map.addSource(id, {
    type: 'geojson',
    data,
  })
}

function ensureLayers(map: maplibregl.Map) {
  if (!map.getLayer(LAYER_IDS.statesFill)) {
    map.addLayer({
      id: LAYER_IDS.statesFill,
      type: 'fill',
      source: SOURCE_IDS.states,
      paint: {
        'fill-color': ['coalesce', ['get', 'fillColor'], '#f7f9fc'],
        'fill-opacity': ['coalesce', ['get', 'fillOpacity'], 0],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.statesLine)) {
    map.addLayer({
      id: LAYER_IDS.statesLine,
      type: 'line',
      source: SOURCE_IDS.states,
      paint: {
        'line-color': ['coalesce', ['get', 'lineColor'], '#8f9db6'],
        'line-width': ['coalesce', ['get', 'lineWidth'], 1],
        'line-opacity': ['coalesce', ['get', 'lineOpacity'], 1],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.lgasLine)) {
    map.addLayer({
      id: LAYER_IDS.lgasLine,
      type: 'line',
      source: SOURCE_IDS.lgas,
      paint: {
        'line-color': ['coalesce', ['get', 'lineColor'], '#7a8ca8'],
        'line-width': ['coalesce', ['get', 'lineWidth'], 1],
        'line-opacity': ['coalesce', ['get', 'lineOpacity'], 1],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.wardsFill)) {
    map.addLayer({
      id: LAYER_IDS.wardsFill,
      type: 'fill',
      source: SOURCE_IDS.wards,
      paint: {
        'fill-color': ['coalesce', ['get', 'fillColor'], '#eb6b3b'],
        'fill-opacity': ['coalesce', ['get', 'fillOpacity'], 0],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.wardsLine)) {
    map.addLayer({
      id: LAYER_IDS.wardsLine,
      type: 'line',
      source: SOURCE_IDS.wards,
      paint: {
        'line-color': ['coalesce', ['get', 'lineColor'], '#d25a2f'],
        'line-width': ['coalesce', ['get', 'lineWidth'], 1],
        'line-opacity': ['coalesce', ['get', 'lineOpacity'], 1],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.focusBoundariesLine)) {
    map.addLayer({
      id: LAYER_IDS.focusBoundariesLine,
      type: 'line',
      source: SOURCE_IDS.focusBoundaries,
      paint: {
        'line-color': ['coalesce', ['get', 'lineColor'], '#15396d'],
        'line-width': ['coalesce', ['get', 'lineWidth'], 4],
        'line-opacity': ['coalesce', ['get', 'lineOpacity'], 1],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.halosFill)) {
    map.addLayer({
      id: LAYER_IDS.halosFill,
      type: 'fill',
      source: SOURCE_IDS.halos,
      paint: {
        'fill-color': ['coalesce', ['get', 'fillColor'], '#2d5eca'],
        'fill-opacity': ['coalesce', ['get', 'fillOpacity'], 0.06],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.halosLine)) {
    map.addLayer({
      id: LAYER_IDS.halosLine,
      type: 'line',
      source: SOURCE_IDS.halos,
      paint: {
        'line-color': ['coalesce', ['get', 'lineColor'], '#2d5eca'],
        'line-width': ['coalesce', ['get', 'lineWidth'], 0.8],
        'line-opacity': ['coalesce', ['get', 'lineOpacity'], 0.28],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.pointTilesCircle) && map.getSource(SOURCE_IDS.pointTiles)) {
    map.addLayer({
      id: LAYER_IDS.pointTilesCircle,
      type: 'circle',
      source: SOURCE_IDS.pointTiles,
      'source-layer': POINT_TILE_LAYER_NAME,
      paint: {
        'circle-color': [
          'match',
          ['get', 'status'],
          'Observation',
          '#e46635',
          'Completed',
          '#24a16f',
          '#1f5fb7',
        ],
        'circle-opacity': 0.84,
        'circle-radius': [
          'interpolate',
          ['linear'],
          ['zoom'],
          5,
          3.4,
          7,
          4.4,
          9,
          5.2,
          12,
          6.2,
          16,
          7.4,
        ],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.pointsCircle) && map.getSource(SOURCE_IDS.points)) {
    map.addLayer({
      id: LAYER_IDS.pointsCircle,
      type: 'circle',
      source: SOURCE_IDS.points,
      paint: {
        'circle-color': ['coalesce', ['get', 'circleColor'], '#1f5fb7'],
        'circle-opacity': ['coalesce', ['get', 'circleOpacity'], 0.86],
        'circle-radius': ['coalesce', ['get', 'circleRadius'], 5.8],
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.selectedOuter) && map.getSource(SOURCE_IDS.selectedPoint)) {
    map.addLayer({
      id: LAYER_IDS.selectedOuter,
      type: 'circle',
      source: SOURCE_IDS.selectedPoint,
      paint: {
        'circle-color': '#15396d',
        'circle-opacity': 0.08,
        'circle-radius': 14,
        'circle-stroke-color': '#15396d',
        'circle-stroke-opacity': 0.45,
        'circle-stroke-width': 1.2,
      },
    })
  }

  if (!map.getLayer(LAYER_IDS.selectedInner) && map.getSource(SOURCE_IDS.selectedPoint)) {
    map.addLayer({
      id: LAYER_IDS.selectedInner,
      type: 'circle',
      source: SOURCE_IDS.selectedPoint,
      paint: {
        'circle-color': '#ffffff',
        'circle-opacity': 1,
        'circle-radius': 6,
        'circle-stroke-color': '#15396d',
        'circle-stroke-opacity': 1,
        'circle-stroke-width': 3,
      },
    })
  }

  bringOverlayLayersToFront(map)
}

function bringOverlayLayersToFront(map: maplibregl.Map) {
  OVERLAY_LAYER_ORDER.forEach((layerId) => {
    if (map.getLayer(layerId)) {
      map.moveLayer(layerId)
    }
  })
}

function removeBasemap(map: maplibregl.Map) {
  const style = map.getStyle()
  if (!style) {
    return
  }

  const layers =
    style.layers?.filter((layer) => layer.id.startsWith(BASEMAP_LAYER_PREFIX)).map((layer) => layer.id) ?? []

  layers.reverse().forEach((id) => {
    if (map.getLayer(id)) {
      map.removeLayer(id)
    }
  })

  Object.keys(style.sources ?? {})
    .filter((id) => id.startsWith(BASEMAP_SOURCE_PREFIX))
    .forEach((id) => {
      if (map.getSource(id)) {
        map.removeSource(id)
      }
    })
}

function syncBasemap(map: maplibregl.Map, basemap: BasemapDefinition) {
  if (!map.isStyleLoaded()) {
    return false
  }

  const backgroundColor = basemap.backgroundColor ?? '#eef2f7'
  if (map.getLayer('background')) {
    map.setPaintProperty('background', 'background-color', backgroundColor)
  }

  removeBasemap(map)

  const beforeId = map.getLayer(LAYER_IDS.statesFill) ? LAYER_IDS.statesFill : undefined

  basemap.layers.forEach((layer, index) => {
    const sourceId = `${BASEMAP_SOURCE_PREFIX}${index}`
    const layerId = `${BASEMAP_LAYER_PREFIX}${index}`

    map.addSource(sourceId, {
      type: 'raster',
      tiles: [layer.url],
      tileSize: 256,
      attribution: layer.attribution,
    })

    map.addLayer(
      {
        id: layerId,
        type: 'raster',
        source: sourceId,
        paint: {
          'raster-opacity': 1,
        },
      },
      beforeId,
    )
  })

  bringOverlayLayersToFront(map)

  return true
}

function removePointTiles(map: maplibregl.Map) {
  if (map.getLayer(LAYER_IDS.pointTilesCircle)) {
    map.removeLayer(LAYER_IDS.pointTilesCircle)
  }

  if (map.getSource(SOURCE_IDS.pointTiles)) {
    map.removeSource(SOURCE_IDS.pointTiles)
  }
}

function syncPointTiles(map: maplibregl.Map, tileUrl: string) {
  if (!map.isStyleLoaded()) {
    return false
  }

  removePointTiles(map)

  if (!tileUrl) {
    return true
  }

  map.addSource(SOURCE_IDS.pointTiles, {
    type: 'vector',
    tiles: [tileUrl],
    minzoom: 0,
    maxzoom: 18,
  })
  ensureLayers(map)
  return true
}

function getPopupHtml(feature: MapGeoJSONFeature | undefined) {
  const properties = feature?.properties as Record<string, unknown> | undefined
  return typeof properties?.popupHtml === 'string' ? properties.popupHtml : ''
}

function escapeHtml(value: unknown) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function buildTiledPointPopupHtml(properties: Record<string, unknown>) {
  const title = escapeHtml(properties.id || 'GPS observation')
  const rows = [
    ['Status', properties.status],
  ]
    .filter(([, value]) => String(value ?? '').trim().length > 0)
    .map(
      ([label, value]) => `
        <div class="map-tooltip__row">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `,
    )
    .join('')

  return `
    <div class="map-tooltip">
      <strong class="map-tooltip__title">${title}</strong>
      <span class="map-tooltip__subtitle">GPS observation</span>
      <div class="map-tooltip__grid">${rows}</div>
    </div>
  `
}

function keepPopupInView(map: maplibregl.Map, popup: Popup, lngLat: LngLat) {
  const popupElement = popup.getElement()
  const container = map.getContainer()
  const containerRect = container.getBoundingClientRect()
  const popupRect = popupElement.getBoundingClientRect()
  const viewportPadding = 18

  let safeLeft = containerRect.left + viewportPadding
  let safeTop = containerRect.top + viewportPadding
  let safeRight = containerRect.right - viewportPadding
  let safeBottom = containerRect.bottom - viewportPadding

  let shiftX = 0
  let shiftY = 0

  if (popupRect.left < safeLeft) {
    shiftX = popupRect.left - safeLeft
  } else if (popupRect.right > safeRight) {
    shiftX = popupRect.right - safeRight
  }

  if (popupRect.top < safeTop) {
    shiftY = popupRect.top - safeTop
  } else if (popupRect.bottom > safeBottom) {
    shiftY = popupRect.bottom - safeBottom
  }

  if (shiftX === 0 && shiftY === 0) {
    return
  }

  const clickedPoint = map.project(lngLat)
  const nextCenter = map.unproject([
    clickedPoint.x + shiftX,
    clickedPoint.y + shiftY,
  ])

  map.easeTo({
    center: nextCenter,
    duration: 280,
    essential: true,
  })
}

function bindInteractiveLayer(
  map: maplibregl.Map,
  popup: Popup,
  layerId: string,
  onClick?: (feature: MapGeoJSONFeature) => void,
) {
  const showPopup = (event: maplibregl.MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    const popupHtml = getPopupHtml(feature)

    if (!feature || !popupHtml) {
      return
    }

    popup.setLngLat(event.lngLat).setHTML(popupHtml)

    if (!popup.isOpen()) {
      popup.addTo(map)
    }

    requestAnimationFrame(() => {
      if (!popup.isOpen()) {
        return
      }

      keepPopupInView(map, popup, event.lngLat)
    })
  }

  const handleMouseEnter = () => {
    map.getCanvas().style.cursor = 'pointer'
  }

  const handleMouseLeave = () => {
    map.getCanvas().style.cursor = ''
  }

  const handleClick = (event: maplibregl.MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    if (!feature) {
      return
    }

    showPopup(event)
    onClick?.(feature)
  }

  map.on('mouseenter', layerId, handleMouseEnter)
  map.on('mouseleave', layerId, handleMouseLeave)
  map.on('click', layerId, handleClick)

  return () => {
    map.off('mouseenter', layerId, handleMouseEnter)
    map.off('mouseleave', layerId, handleMouseLeave)
    map.off('click', layerId, handleClick)
  }
}

function bindTiledPointLayer(map: maplibregl.Map, popup: Popup, layerId: string) {
  const handleMouseEnter = () => {
    map.getCanvas().style.cursor = 'pointer'
  }

  const handleMouseLeave = () => {
    map.getCanvas().style.cursor = ''
  }

  const handleClick = (event: maplibregl.MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    const properties = feature?.properties as Record<string, unknown> | undefined
    if (!feature || !properties) {
      return
    }

    popup.setLngLat(event.lngLat).setHTML(buildTiledPointPopupHtml(properties))
    if (!popup.isOpen()) {
      popup.addTo(map)
    }

    requestAnimationFrame(() => {
      if (!popup.isOpen()) {
        return
      }

      keepPopupInView(map, popup, event.lngLat)
    })
  }

  map.on('mouseenter', layerId, handleMouseEnter)
  map.on('mouseleave', layerId, handleMouseLeave)
  map.on('click', layerId, handleClick)

  return () => {
    map.off('mouseenter', layerId, handleMouseEnter)
    map.off('mouseleave', layerId, handleMouseLeave)
    map.off('click', layerId, handleClick)
  }
}

function updateSourceData(map: maplibregl.Map, id: string, data: FeatureCollection) {
  const source = map.getSource(id) as GeoJSONSource | undefined
  source?.setData(data)
}

function useLatest<T>(value: T): MutableRefObject<T> {
  const ref = useRef(value)

  useEffect(() => {
    ref.current = value
  }, [value])

  return ref
}

export default function CoverageMap({
  basemap,
  pointTileUrl,
  stateData,
  lgaData,
  wardData,
  focusBoundaryData,
  haloData,
  pointData,
  selectedPointData,
  viewportBounds,
  viewportMaxZoom,
  selectedPoint,
  onStateSelect,
  onLgaSelect,
  onWardSelect,
  onViewportChange,
}: CoverageMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const popupRef = useRef<Popup | null>(null)
  const cleanupRef = useRef<Array<() => void>>([])
  const pointTileCleanupRef = useRef<(() => void) | null>(null)
  const basemapIdRef = useRef('')
  const pointTileUrlRef = useRef('')
  const lastViewportTargetRef = useRef<{ bounds: MapBounds; maxZoom: number } | null>(null)
  const lastSelectedPointRef = useRef<FocusPoint | null>(null)
  const [isReady, setIsReady] = useState(false)
  const latestStateSelect = useLatest(onStateSelect)
  const latestLgaSelect = useLatest(onLgaSelect)
  const latestWardSelect = useLatest(onWardSelect)
  const latestViewportChange = useLatest(onViewportChange)

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return
    }

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: EMPTY_STYLE,
      center: [8.6753, 9.082],
      zoom: 6,
      minZoom: 5,
      maxZoom: 18,
      dragRotate: true,
      touchZoomRotate: true,
      pitchWithRotate: false,
      renderWorldCopies: false,
    })

    map.addControl(
      new NavigationControl({
        showCompass: true,
        visualizePitch: false,
      }),
      'top-left',
    )

    popupRef.current = new Popup({
      closeButton: true,
      closeOnClick: true,
      maxWidth: '320px',
      className: 'map-tooltip-shell',
    })

    map.on('load', () => {
      ensureGeoJsonSource(map, SOURCE_IDS.states, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.lgas, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.wards, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.focusBoundaries, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.halos, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.points, createEmptyCollection())
      ensureGeoJsonSource(map, SOURCE_IDS.selectedPoint, createEmptyCollection())
      ensureLayers(map)
      if (syncPointTiles(map, pointTileUrl)) {
        pointTileUrlRef.current = pointTileUrl
      }
      if (syncBasemap(map, basemap)) {
        basemapIdRef.current = basemap.id
      }

      cleanupRef.current = [
        bindInteractiveLayer(map, popupRef.current!, LAYER_IDS.statesFill, (feature) => {
          const properties = feature.properties as Record<string, unknown>
          const stateName = properties.stateName
          if (typeof stateName === 'string') {
            latestStateSelect.current(stateName)
          }
        }),
        bindInteractiveLayer(map, popupRef.current!, LAYER_IDS.lgasLine, (feature) => {
          const properties = feature.properties as Record<string, unknown>
          const stateName = properties.stateName
          const lgaName = properties.lgaName

          if (typeof stateName === 'string' && typeof lgaName === 'string') {
            latestLgaSelect.current(stateName, lgaName)
          }
        }),
        bindInteractiveLayer(map, popupRef.current!, LAYER_IDS.wardsFill, (feature) => {
          const properties = feature.properties as Record<string, unknown>
          const wardKey = properties.wardKey
          if (typeof wardKey === 'string') {
            latestWardSelect.current(wardKey)
          }
        }),
        bindInteractiveLayer(map, popupRef.current!, LAYER_IDS.pointsCircle),
      ]
      if (map.getLayer(LAYER_IDS.pointTilesCircle)) {
        pointTileCleanupRef.current = bindTiledPointLayer(
          map,
          popupRef.current!,
          LAYER_IDS.pointTilesCircle,
        )
      }

      map.on('moveend', () => {
        const bounds = map.getBounds()
        latestViewportChange.current(
          [
            [bounds.getWest(), bounds.getSouth()],
            [bounds.getEast(), bounds.getNorth()],
          ],
          map.getZoom(),
        )
      })

      setIsReady(true)
    })

    mapRef.current = map

    return () => {
      pointTileCleanupRef.current?.()
      pointTileCleanupRef.current = null
      cleanupRef.current.forEach((cleanup) => cleanup())
      cleanupRef.current = []
      popupRef.current?.remove()
      popupRef.current = null
      basemapIdRef.current = ''
      pointTileUrlRef.current = ''
      map.remove()
      mapRef.current = null
    }
  }, [latestLgaSelect, latestStateSelect, latestWardSelect, latestViewportChange])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !isReady) {
      return
    }

    if (basemapIdRef.current === basemap.id) {
      return
    }

    const applyBasemap = () => {
      if (mapRef.current !== map) {
        return
      }

      if (!syncBasemap(map, basemap)) {
        map.once('styledata', applyBasemap)
        return
      }

      basemapIdRef.current = basemap.id
    }

    applyBasemap()

    return () => {
      map.off('styledata', applyBasemap)
    }
  }, [basemap, isReady])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !isReady) {
      return
    }

    if (pointTileUrlRef.current === pointTileUrl) {
      return
    }

    const applyPointTiles = () => {
      if (mapRef.current !== map) {
        return
      }

      if (!syncPointTiles(map, pointTileUrl)) {
        return
      }

      map.off('styledata', applyPointTiles)
      pointTileUrlRef.current = pointTileUrl

      if (popupRef.current && map.getLayer(LAYER_IDS.pointTilesCircle)) {
        pointTileCleanupRef.current = bindTiledPointLayer(
          map,
          popupRef.current,
          LAYER_IDS.pointTilesCircle,
        )
      }
    }

    pointTileCleanupRef.current?.()
    pointTileCleanupRef.current = null
    applyPointTiles()

    if (pointTileUrlRef.current !== pointTileUrl) {
      map.on('styledata', applyPointTiles)
    }

    return () => {
      map.off('styledata', applyPointTiles)
    }
  }, [isReady, pointTileUrl])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !isReady) {
      return
    }

    updateSourceData(map, SOURCE_IDS.states, stateData)
    updateSourceData(map, SOURCE_IDS.lgas, lgaData)
    updateSourceData(map, SOURCE_IDS.wards, wardData)
    updateSourceData(map, SOURCE_IDS.focusBoundaries, focusBoundaryData)
    updateSourceData(map, SOURCE_IDS.halos, haloData)
    updateSourceData(map, SOURCE_IDS.points, pointData)
    updateSourceData(map, SOURCE_IDS.selectedPoint, selectedPointData)
  }, [focusBoundaryData, haloData, isReady, lgaData, pointData, selectedPointData, stateData, wardData])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !isReady) {
      return
    }

    if (selectedPoint) {
      const previousPoint = lastSelectedPointRef.current
      if (
        previousPoint &&
        previousPoint.id === selectedPoint.id &&
        numbersAreClose(previousPoint.latitude, selectedPoint.latitude) &&
        numbersAreClose(previousPoint.longitude, selectedPoint.longitude)
      ) {
        return
      }

      lastSelectedPointRef.current = selectedPoint
      lastViewportTargetRef.current = null
      map.flyTo({
        center: [selectedPoint.longitude, selectedPoint.latitude],
        zoom: 17,
        duration: 850,
        essential: true,
      })
      return
    }

    if (!viewportBounds) {
      return
    }

    lastSelectedPointRef.current = null
    const previousViewport = lastViewportTargetRef.current
    if (
      previousViewport &&
      previousViewport.maxZoom === viewportMaxZoom &&
      boundsAreEqual(previousViewport.bounds, viewportBounds)
    ) {
      return
    }

    lastViewportTargetRef.current = {
      bounds: viewportBounds,
      maxZoom: viewportMaxZoom,
    }
    map.fitBounds(viewportBounds as LngLatBoundsLike, {
      padding: {
        top: 44,
        right: 44,
        bottom: 44,
        left: 44,
      },
      maxZoom: viewportMaxZoom,
      duration: 850,
      essential: true,
    })
  }, [isReady, selectedPoint, viewportBounds, viewportMaxZoom])

  return <div ref={containerRef} className="map-canvas" />
}
