import dayjs from 'dayjs'

export const fmtTime = (t?: string | null): string => {
  if (!t) return '-'
  return dayjs(t).format('YYYY-MM-DD HH:mm:ss')
}

export const fmtDuration = (ms?: number | null): string => {
  if (!ms) return '-'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

export const copyText = async (text: string): Promise<void> => {
  await navigator.clipboard.writeText(text)
}
