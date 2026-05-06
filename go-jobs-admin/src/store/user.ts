import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { User } from '@/api'
import { authApi } from '@/api'

export const useUserStore = defineStore('user', () => {
  const user = ref<User | null>(null)
  const token = ref<string>(localStorage.getItem('token') ?? '')

  const setToken = (t: string) => {
    token.value = t
    localStorage.setItem('token', t)
  }

  const fetchMe = async () => {
    try {
      user.value = await authApi.me()
    } catch {}
  }

  const logout = () => {
    token.value = ''
    user.value = null
    localStorage.removeItem('token')
  }

  const isAdmin = () => user.value?.role === 1

  return { user, token, setToken, fetchMe, logout, isAdmin }
})
