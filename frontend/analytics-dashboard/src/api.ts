export type SalesSummary = {
  totalOrders: number;
  totalItems: number;
  totalRevenue: number;
  averageOrderValue: number;
  uniqueCustomers: number;
  uniqueProducts: number;
  goldProcessedAt: string;
  refreshedAt: string;
};

export type DailySales = {
  eventDate: string;
  channel: string;
  location: string;
  totalOrders: number;
  totalItems: number;
  totalRevenue: number;
  averageOrderValue: number;
  goldProcessedAt: string;
  refreshedAt: string;
};

const API_URL =
  import.meta.env.VITE_API_URL
  ?? "http://localhost:8082";

async function getJson<T>(
  path: string
): Promise<T> {
  const response = await fetch(
    `${API_URL}${path}`
  );

  if (!response.ok) {
    throw new Error(
      `HTTP ${response.status}: ${response.statusText}`
    );
  }

  return response.json() as Promise<T>;
}

export function getSummary() {
  return getJson<SalesSummary>(
    "/api/v1/analytics/summary"
  );
}

export function getDailySales() {
  return getJson<DailySales[]>(
    "/api/v1/analytics/sales/daily?limit=100"
  );
}
