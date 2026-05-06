import { ref, computed } from 'vue'

export function usePagination(defaultPageSize = 20) {
  const page = ref(1)
  const pageSize = ref(defaultPageSize)
  const total = ref(0)

  const totalPages = computed(() => Math.ceil(total.value / pageSize.value))
  const pageNumbers = computed(() =>
    Array.from({ length: Math.min(totalPages.value, 10) }, (_, i) => i + 1)
  )

  const reset = () => { page.value = 1 }

  return { page, pageSize, total, totalPages, pageNumbers, reset }
}
