import {
  useEffect,
  useMemo,
  useState
} from "react";
import {
  DailySales,
  SalesSummary,
  getDailySales,
  getSummary
} from "./api";

function currency(
  value: number
) {
  return new Intl.NumberFormat(
    "pt-BR",
    {
      style: "currency",
      currency: "BRL"
    }
  ).format(value);
}

function number(
  value: number
) {
  return new Intl.NumberFormat(
    "pt-BR"
  ).format(value);
}

function RevenueChart({
  rows
}: {
  rows: DailySales[];
}) {
  const maxRevenue = Math.max(
    ...rows.map(
      (row) => row.totalRevenue
    ),
    1
  );

  if (rows.length === 0) {
    return (
      <div className="empty-state">
        Nenhum dado disponível.
      </div>
    );
  }

  return (
    <div className="chart">
      {rows.slice(0, 12).map(
        (row) => {
          const width =
            (row.totalRevenue / maxRevenue)
            * 100;

          return (
            <div
              className="chart-row"
              key={
                `${row.eventDate}-${row.channel}-${row.location}`
              }
            >
              <div className="chart-label">
                <strong>
                  {row.eventDate}
                </strong>
                <span>
                  {row.channel}
                  {" • "}
                  {row.location}
                </span>
              </div>

              <div className="bar-track">
                <div
                  className="bar"
                  style={{
                    width:
                      `${Math.max(
                        width,
                        4
                      )}%`
                  }}
                />
              </div>

              <div className="chart-value">
                {currency(
                  row.totalRevenue
                )}
              </div>
            </div>
          );
        }
      )}
    </div>
  );
}

function App() {
  const [
    summary,
    setSummary
  ] = useState<SalesSummary | null>(
    null
  );

  const [
    daily,
    setDaily
  ] = useState<DailySales[]>([]);

  const [
    loading,
    setLoading
  ] = useState(true);

  const [
    error,
    setError
  ] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      getSummary(),
      getDailySales()
    ])
      .then(
        ([
          summaryData,
          dailyData
        ]) => {
          setSummary(summaryData);
          setDaily(dailyData);
        }
      )
      .catch((cause) => {
        setError(
          cause instanceof Error
            ? cause.message
            : "Falha ao carregar dados."
        );
      })
      .finally(() => {
        setLoading(false);
      });
  }, []);

  const refreshedAt = useMemo(
    () => {
      if (!summary) {
        return "—";
      }

      return new Intl.DateTimeFormat(
        "pt-BR",
        {
          dateStyle: "short",
          timeStyle: "medium"
        }
      ).format(
        new Date(
          summary.refreshedAt
        )
      );
    },
    [summary]
  );

  if (loading) {
    return (
      <main className="center-state">
        Carregando SmartRetail Analytics...
      </main>
    );
  }

  if (error || !summary) {
    return (
      <main className="center-state">
        <section className="error-card">
          <h1>
            Analytics indisponível
          </h1>
          <p>
            {error
              ?? "Resumo analítico não encontrado."}
          </p>
        </section>
      </main>
    );
  }

  const cards = [
    {
      label: "Receita total",
      value: currency(
        summary.totalRevenue
      )
    },
    {
      label: "Pedidos",
      value: number(
        summary.totalOrders
      )
    },
    {
      label: "Ticket médio",
      value: currency(
        summary.averageOrderValue
      )
    },
    {
      label: "Itens vendidos",
      value: number(
        summary.totalItems
      )
    },
    {
      label: "Clientes únicos",
      value: number(
        summary.uniqueCustomers
      )
    },
    {
      label: "Produtos únicos",
      value: number(
        summary.uniqueProducts
      )
    }
  ];

  return (
    <main className="page">
      <header className="hero">
        <div>
          <span className="eyebrow">
            SMARTRETAIL DATA PLATFORM
          </span>
          <h1>
            Analytics Dashboard
          </h1>
          <p>
            Data Products Gold servidos
            por Java 21 + Spring Boot.
          </p>
        </div>

        <div className="status">
          <span className="status-dot" />
          Atualizado em {refreshedAt}
        </div>
      </header>

      <section className="kpi-grid">
        {cards.map((card) => (
          <article
            className="kpi-card"
            key={card.label}
          >
            <span>
              {card.label}
            </span>
            <strong>
              {card.value}
            </strong>
          </article>
        ))}
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <span className="eyebrow">
              GOLD DATA PRODUCT
            </span>
            <h2>
              Receita por segmento
            </h2>
          </div>
        </div>

        <RevenueChart
          rows={daily}
        />
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <span className="eyebrow">
              DAILY SALES
            </span>
            <h2>
              Detalhamento analítico
            </h2>
          </div>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Data</th>
                <th>Canal</th>
                <th>Local</th>
                <th>Pedidos</th>
                <th>Itens</th>
                <th>Receita</th>
                <th>Ticket médio</th>
              </tr>
            </thead>

            <tbody>
              {daily.map((row) => (
                <tr
                  key={
                    `${row.eventDate}-${row.channel}-${row.location}`
                  }
                >
                  <td>
                    {row.eventDate}
                  </td>
                  <td>
                    {row.channel}
                  </td>
                  <td>
                    {row.location}
                  </td>
                  <td>
                    {number(
                      row.totalOrders
                    )}
                  </td>
                  <td>
                    {number(
                      row.totalItems
                    )}
                  </td>
                  <td>
                    {currency(
                      row.totalRevenue
                    )}
                  </td>
                  <td>
                    {currency(
                      row.averageOrderValue
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

export default App;
