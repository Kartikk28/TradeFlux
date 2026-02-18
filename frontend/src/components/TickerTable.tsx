
export default function TickerTable({ rows }: { rows: { label: string, value: string | number | null }[] }) {
  return (
    <table className="table">
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            <th>{r.label}</th>
            <td>{r.value ?? '-'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
