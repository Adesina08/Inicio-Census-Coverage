import type { Feature, Position } from 'geojson'
import type { AreaGeometry, CoverageStatus, ObservationFeature, WardFeature } from '../data/coverage'

export type MapBounds = [[number, number], [number, number]]

export type CoverageCell = {
  covered: boolean
  bounds: MapBounds
}

type GeometryBounds = {
  south: number
  west: number
  north: number
  east: number
}

const EARTH_RADIUS_METERS = 6_371_000
const PREVIEW_TARGET_CELLS = 2600

export const GPS_TOLERANCE_METERS = 10
export const GRID_CELL_METERS = 10
export const NEAR_TARGET_PERCENT = 80
export const WELL_COVERED_PERCENT = 85

function toRadians(value: number) {
  return (value * Math.PI) / 180
}

function metersToLatitudeDegrees(meters: number) {
  return (meters / EARTH_RADIUS_METERS) * (180 / Math.PI)
}

function metersToLongitudeDegrees(meters: number, latitude: number) {
  const latitudeCosine = Math.max(Math.abs(Math.cos(toRadians(latitude))), 1e-9)
  return (meters / (EARTH_RADIUS_METERS * latitudeCosine)) * (180 / Math.PI)
}

function appendCoordinateBounds(position: Position, bounds: GeometryBounds) {
  const [longitude, latitude] = position
  bounds.west = Math.min(bounds.west, longitude)
  bounds.south = Math.min(bounds.south, latitude)
  bounds.east = Math.max(bounds.east, longitude)
  bounds.north = Math.max(bounds.north, latitude)
}

function walkCoordinates(coordinates: Position[] | Position[][] | Position[][][], bounds: GeometryBounds) {
  if (typeof coordinates[0][0] === 'number') {
    ;(coordinates as Position[]).forEach((position) => appendCoordinateBounds(position, bounds))
    return
  }

  ;(coordinates as Position[][] | Position[][][]).forEach((child) =>
    walkCoordinates(child as Position[] | Position[][] | Position[][][], bounds),
  )
}

function createEmptyBounds(): GeometryBounds {
  return {
    south: Number.POSITIVE_INFINITY,
    west: Number.POSITIVE_INFINITY,
    north: Number.NEGATIVE_INFINITY,
    east: Number.NEGATIVE_INFINITY,
  }
}

function ringContainsPoint(position: Position, ring: Position[]) {
  const [longitude, latitude] = position
  let inside = false

  for (let index = 0, previous = ring.length - 1; index < ring.length; previous = index, index += 1) {
    const [currentLongitude, currentLatitude] = ring[index]
    const [previousLongitude, previousLatitude] = ring[previous]

    const intersects =
      currentLatitude > latitude !== previousLatitude > latitude &&
      longitude <
        ((previousLongitude - currentLongitude) * (latitude - currentLatitude)) /
          (previousLatitude - currentLatitude) +
          currentLongitude

    if (intersects) {
      inside = !inside
    }
  }

  return inside
}

function polygonContainsPoint(position: Position, rings: Position[][]) {
  if (!rings[0] || !ringContainsPoint(position, rings[0])) {
    return false
  }

  for (let index = 1; index < rings.length; index += 1) {
    if (ringContainsPoint(position, rings[index])) {
      return false
    }
  }

  return true
}

function distanceMeters(first: [number, number], second: [number, number]) {
  const [latitudeOne, longitudeOne] = first
  const [latitudeTwo, longitudeTwo] = second
  const latitudeDelta = toRadians(latitudeTwo - latitudeOne)
  const longitudeDelta = toRadians(longitudeTwo - longitudeOne)
  const firstLatitudeRadians = toRadians(latitudeOne)
  const secondLatitudeRadians = toRadians(latitudeTwo)

  const haversine =
    Math.sin(latitudeDelta / 2) ** 2 +
    Math.cos(firstLatitudeRadians) *
      Math.cos(secondLatitudeRadians) *
      Math.sin(longitudeDelta / 2) ** 2

  return 2 * EARTH_RADIUS_METERS * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine))
}

function getPreviewCellSizeMeters(totalCells: number) {
  if (!totalCells || totalCells <= PREVIEW_TARGET_CELLS) {
    return GRID_CELL_METERS
  }

  const scaledSize = GRID_CELL_METERS * Math.sqrt(totalCells / PREVIEW_TARGET_CELLS)
  return Math.max(GRID_CELL_METERS, Math.ceil(scaledSize / 10) * 10)
}

export function getCoverageStatus(coveragePercent: number): CoverageStatus {
  if (coveragePercent >= WELL_COVERED_PERCENT) {
    return 'well_covered'
  }

  if (coveragePercent >= NEAR_TARGET_PERCENT) {
    return 'near_target'
  }

  return 'under_covered'
}

export function geometryContainsPoint(position: Position, geometry: AreaGeometry) {
  if (geometry.type === 'Polygon') {
    return polygonContainsPoint(position, geometry.coordinates)
  }

  return geometry.coordinates.some((polygon) => polygonContainsPoint(position, polygon))
}

export function getGeometryBounds(geometry: AreaGeometry): GeometryBounds {
  const bounds = createEmptyBounds()
  walkCoordinates(geometry.coordinates, bounds)
  return bounds
}

export function getFeatureBounds(feature: Feature<AreaGeometry> | null | undefined): MapBounds | null {
  if (!feature) {
    return null
  }

  const bounds = getGeometryBounds(feature.geometry)
  return [
    [bounds.west, bounds.south],
    [bounds.east, bounds.north],
  ]
}

export function getCollectionBounds(
  features: Array<Feature<AreaGeometry> | null | undefined>,
): MapBounds | null {
  const validFeatures = features.filter(Boolean) as Feature<AreaGeometry>[]

  if (validFeatures.length === 0) {
    return null
  }

  const combined = createEmptyBounds()

  validFeatures.forEach((feature) => {
    const bounds = getGeometryBounds(feature.geometry)
    combined.south = Math.min(combined.south, bounds.south)
    combined.west = Math.min(combined.west, bounds.west)
    combined.north = Math.max(combined.north, bounds.north)
    combined.east = Math.max(combined.east, bounds.east)
  })

  return [
    [combined.west, combined.south],
    [combined.east, combined.north],
  ]
}

export function getObservationLatLng(feature: ObservationFeature): [number, number] {
  const [longitude, latitude] = feature.geometry.coordinates
  return [latitude, longitude]
}

export function buildWardCoveragePreview(ward: WardFeature, points: ObservationFeature[]) {
  if (!points.length) {
    return {
      cells: [] as CoverageCell[],
      previewCellSizeMeters: getPreviewCellSizeMeters(ward.properties.totalCells),
    }
  }

  const previewCellSizeMeters = getPreviewCellSizeMeters(ward.properties.totalCells)
  const latitudeStep = metersToLatitudeDegrees(previewCellSizeMeters)
  const bounds = getGeometryBounds(ward.geometry)
  const cells: CoverageCell[] = []
  const pointCoordinates = points.map((feature) => getObservationLatLng(feature))

  for (
    let latitude = bounds.south + latitudeStep / 2;
    latitude < bounds.north;
    latitude += latitudeStep
  ) {
    const longitudeStep = metersToLongitudeDegrees(previewCellSizeMeters, latitude)

    for (
      let longitude = bounds.west + longitudeStep / 2;
      longitude < bounds.east;
      longitude += longitudeStep
    ) {
      if (!geometryContainsPoint([longitude, latitude], ward.geometry)) {
        continue
      }

      const covered = pointCoordinates.some(
        ([pointLatitude, pointLongitude]) =>
          distanceMeters([latitude, longitude], [pointLatitude, pointLongitude]) <=
          GPS_TOLERANCE_METERS,
      )

      cells.push({
        covered,
        bounds: [
          [longitude - longitudeStep / 2, latitude - latitudeStep / 2],
          [longitude + longitudeStep / 2, latitude + latitudeStep / 2],
        ],
      })
    }
  }

  return {
    cells,
    previewCellSizeMeters,
  }
}
